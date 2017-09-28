//! Flush operations writer

use std::cmp;
use std::iter::Peekable;

use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};

use error::Result;
use key::Key;
use metadata::Metadata;
use record::{append_record, Record};
use space::{SpaceIterator, Space, EmptySpace, OccupiedSpace};
use transaction::Operation;

struct InsertOperation<'a> {
	/// Key of the record which needs to be inserted
	key: Key<'a>,
	/// Value of the record which needs to be inserted
	value: &'a [u8],
}

impl<'a> InsertOperation<'a> {
	/// Returns operation length
	fn write(&self, buffer: &mut Vec<u8>, field_body_size: usize, const_value: bool) -> usize {
		let buffer_len = buffer.len();
		append_record(buffer, self.key.key, self.value, field_body_size, const_value);
		buffer.len() - buffer_len
	}
}

struct OverwriteOperation<'a> {
	/// Key of the record which needs to be inserted
	key: Key<'a>,
	/// Value of the record which needs to be inserted
	value: &'a [u8],
	/// Length of the record which needs to be overwritten
	old_len: usize,
}

impl<'a> OverwriteOperation<'a> {
	/// Returns operation length
	fn write(&self, buffer: &mut Vec<u8>, field_body_size: usize, const_value: bool) -> usize {
		let buffer_len = buffer.len();
		append_record(buffer, self.key.key, self.value, field_body_size, const_value);
		let written = buffer.len() - buffer_len;
		if self.old_len > written {
			let new_len = buffer.len() + self.old_len - written;
			buffer.resize(new_len, 3);
		}
		buffer.len() - buffer_len
	}
}

struct DeleteOperation<'a> {
	/// Key of the record which needs to be deleted
	key: Key<'a>,
	/// Length of the record which needs to be deleted
	len: usize,
}

impl<'a> DeleteOperation<'a> {
	/// Returns operation length
	fn write(&self, buffer: &mut Vec<u8>) -> usize {
		let buffer_len = buffer.len();
		buffer.resize(buffer_len + self.len, 3);
		self.len
	}
}

#[derive(Debug, PartialEq, Default)]
struct OperationBuffer {
	inner: Vec<u8>,
	denoted_operation_start: Option<usize>,
}

impl OperationBuffer {
	#[inline]
	fn as_raw_mut(&mut self) -> &mut Vec<u8> {
		&mut self.inner
	}

	#[inline]
	fn denote_operation_start(&mut self, offset: u64) {
		assert!(self.denoted_operation_start.is_none(), "OperationWriter entered incorrect state");
		self.denoted_operation_start = Some(self.inner.len());
		self.inner.write_u64::<LittleEndian>(offset).unwrap();
		// reserve space for len
		self.inner.extend_from_slice(&[0; 4]);
	}

	#[inline]
	fn finish_operation(&mut self) {
		if let Some(operation_start) = self.denoted_operation_start.take() {
			let len = self.inner.len() - (operation_start + 12);
			LittleEndian::write_u32(&mut self.inner[operation_start + 8..operation_start + 12], len as u32);
		}
	}
}

enum OperationWriterState<'op, 'db> {
	ConsumeNextOperation,
	InsertOperation(InsertOperation<'op>, usize),
	OverwriteOperation(OverwriteOperation<'op>, usize),
	DeleteOperation(DeleteOperation<'op>, usize),
	Advance(usize),
	EmptySpace(EmptySpace, usize),
	OccupiedSpace(OccupiedSpace<'db>, usize),
	Finished,
}

enum OperationWriterStep {
	Stepped,
	Finished
}

/// Writes transactions as a set of idempotent operations
pub struct OperationWriter<'op, 'db, I: Iterator> {
	state: OperationWriterState<'op, 'db>,
	operations: Peekable<I>,
	spaces: SpaceIterator<'db>,
	metadata: &'db mut Metadata,
	buffer: OperationBuffer,
	field_body_size: usize,
	prefix_bits: u8,
	const_value: bool,
}

impl<'op, 'db, I: Iterator<Item = Operation<'op>>> OperationWriter<'op, 'db, I> {
	/// Creates new operations writer. All operations needs to be ordered by key.
	pub fn new(
		operations: I,
		database: &'db [u8],
		metadata: &'db mut Metadata,
		field_body_size: usize,
		prefix_bits: u8,
		const_value: bool,
	) -> Self {
		OperationWriter {
			state: OperationWriterState::ConsumeNextOperation,
			operations: operations.peekable(),
			spaces: SpaceIterator::new(database, field_body_size, 0),
			metadata,
			buffer: OperationBuffer::default(),
			field_body_size,
			prefix_bits,
			const_value,
		}
	}

	fn step(&mut self) -> Result<OperationWriterStep> {
		let next_state = match self.state {
			OperationWriterState::ConsumeNextOperation => {
				// write the len of previous operation
				self.buffer.finish_operation();

				// get next operation
				match self.operations.next() {
					// if there is no operation, finish
					None => OperationWriterState::Finished,
					Some(operation) => match operation {
						// if it's insert, peek and move to space offset until you find insertion place
						Operation::Insert(k, value) => {
							println!("insert operation: {:?}", k);
							let key = Key::new(k, self.prefix_bits);
							self.spaces.move_offset_forward(key.offset(self.field_body_size));

							let (offset, previous_size) = loop {
								let space = self.spaces.peek().expect("TODO: db end")?;

								match space {
									Space::Empty(ref space) => {
										break (space.offset, None);
									},
									Space::Occupied(ref space) => {
										match Record::extract_key(&space.data, self.field_body_size, k.len()).partial_cmp(&k).unwrap() {
											cmp::Ordering::Less => {
												// seek
												let _ = self.spaces.next();
											},
											cmp::Ordering::Equal => {
												// overwrite
												let _ = self.spaces.next();
												break (space.offset, Some(space.data.len()));
											},
											cmp::Ordering::Greater => {
												// insert then write old record
												break (space.offset, None);
											}
										}
									}
								}
							};

							// denote operation start
							self.buffer.denote_operation_start(offset as u64);

							// call insert operation
							if let Some(size) = previous_size {
								let overwrite = OverwriteOperation {
									key,
									value,
									old_len: size,
								};
								OperationWriterState::OverwriteOperation(overwrite, 0)
							} else {
								let insert = InsertOperation {
									key,
									value,
								};
								OperationWriterState::InsertOperation(insert, 0)
							}
						},
						Operation::Delete(k) => {
							// if it's delete peek and move to delete place
							// if not found, move to next operation
							let key = Key::new(k, self.prefix_bits);
							self.spaces.move_offset_forward(key.offset(self.field_body_size));

							let next_state = loop {
								let space = self.spaces.peek().expect("TODO: db end")?;

								println!("key to delete: {:?}", k);
								println!("key offset: {:?}", key.offset(self.field_body_size));
								println!("space to delete: {:?}", space);
								match space {
									Space::Empty(_) => {
										// not found
										break OperationWriterState::ConsumeNextOperation;
									},
									Space::Occupied(ref space) => {
										match Record::extract_key(&space.data, self.field_body_size, k.len()).partial_cmp(&k).unwrap() {
											cmp::Ordering::Less => {
												// seek
												let _ = self.spaces.next();
											},
											cmp::Ordering::Equal => {
												let _ = self.spaces.next();

												self.buffer.denote_operation_start(space.offset as u64);
												break OperationWriterState::DeleteOperation(DeleteOperation {
													key,
													len: space.data.len(),
												}, 0)
											},
											cmp::Ordering::Greater => {
												// not found
												break OperationWriterState::ConsumeNextOperation;
											}
										}
									},
								}
							};

							next_state
						},
					}
				}
			},
			OperationWriterState::InsertOperation(ref operation, len) => {
				// write next operation to a buffer
				let written = operation.write(self.buffer.as_raw_mut(), self.field_body_size, self.const_value);
				// update metadata
				self.metadata.insert_record(operation.key.prefix, written);
				// increase the len size by operation size
				// advance to the next operation
				OperationWriterState::Advance(len + written)
			},
			OperationWriterState::OverwriteOperation(ref operation, len) => {
				// write overwrite operation to a buffer
				let written = operation.write(self.buffer.as_raw_mut(), self.field_body_size, self.const_value);
				assert!(written >= operation.old_len, "old record has not been overwritten");
				// update metadata
				self.metadata.update_record_len(operation.old_len, written);
				// len should not be increased
				// advance to the next operation
				OperationWriterState::Advance(len + written - operation.old_len)
			},
			OperationWriterState::DeleteOperation(ref operation, len) => {
				// write to buffer deleted fields
				let _ = operation.write(self.buffer.as_raw_mut());

				// update metadata
				self.metadata.remove_record(operation.key.prefix, operation.len);

				// len should not be increased
				// advance to the next operation
				OperationWriterState::Advance(len)
			},
			OperationWriterState::Advance(len) => {
				// peek next space and operation and decide which one should go first
				let space = self.spaces.peek().expect("TODO: db end")?;

				if len == 0 {
					OperationWriterState::ConsumeNextOperation
				} else {
					match space {
						Space::Empty(space) => {
							// remove this space
							let _ = self.spaces.next();
							OperationWriterState::EmptySpace(space, len)
						},
						Space::Occupied(space) => match self.operations.peek().cloned() {
							None => {
								// remove his space
								let _ = self.spaces.next();
								OperationWriterState::OccupiedSpace(space, len)
							},
							Some(Operation::Insert(key, value)) => {
								let key = Key::new(key, self.prefix_bits);
								match Record::extract_key(&space.data, self.field_body_size, key.key.len()).partial_cmp(&key.key).unwrap() {
									// existing record is smaller
									cmp::Ordering::Less => {
										// rewrite this space
										let _ = self.spaces.next();
										OperationWriterState::OccupiedSpace(space, len)
									}
									// replace records
									cmp::Ordering::Equal => {
										let _ = self.spaces.next();
										let _ = self.operations.next();
										let overwrite = OverwriteOperation {
											key,
											value,
											old_len: space.data.len(),
										};
										OperationWriterState::OverwriteOperation(overwrite, len)
									},
									// existing record is greater, insert new record first
									cmp::Ordering::Greater => {
										let _ = self.operations.next();
										let insert = InsertOperation {
											key,
											value,
										};
										OperationWriterState::InsertOperation(insert, len)
									},
								}
							},
							Some(Operation::Delete(ref key)) => {
								let key = Key::new(key, self.prefix_bits);
								match Record::extract_key(&space.data, self.field_body_size, key.key.len()).partial_cmp(&key.key).unwrap() {
									cmp::Ordering::Less => {
										// rewrite this space
										let _ = self.spaces.next();
										OperationWriterState::OccupiedSpace(space, len)
									},
									cmp::Ordering::Equal => {
										let _ = self.spaces.next();
										let _ = self.operations.next();
										let delete = DeleteOperation {
											key,
											len: space.data.len(),
										};
										OperationWriterState::DeleteOperation(delete, len)
									},
									cmp::Ordering::Greater => {
										// nothing to delete
										let _ = self.operations.next();
										// check next operation
										OperationWriterState::Advance(len)
									},
								}
							},
						}
					}
				}
			},
			OperationWriterState::EmptySpace(ref mut space, len) => {
				// advance to the next operation
				OperationWriterState::Advance(len - space.len)
			},
			OperationWriterState::OccupiedSpace(ref mut space, len) => {
				// write it to a buffer
				self.buffer.as_raw_mut().extend_from_slice(space.data);
				// advance to the next operation
				OperationWriterState::Advance(len)
			},
			OperationWriterState::Finished => {
				return Ok(OperationWriterStep::Finished);
			},
		};

		self.state = next_state;
		Ok(OperationWriterStep::Stepped)
	}

	#[inline]
	pub fn run(mut self) -> Result<Vec<u8>> {
		while let OperationWriterStep::Stepped = self.step()? {}
		let mut result = self.buffer.inner;
		let meta = self.metadata.as_bytes();
		let old_len = result.len();
		result.resize(old_len + meta.len(), 0);
		meta.copy_to_slice(&mut result[old_len..]);
		Ok(result)
	}
}
