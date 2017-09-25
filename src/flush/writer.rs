//! Flush operations writer

use std::cmp;
use std::io::Write;
use std::iter::Peekable;

use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};

use error::Result;
use key::Key;
use record::{append_record, Record};
use space::{SpaceIterator, Space, EmptySpace, OccupiedSpace};
use transaction::Operation;

struct InsertOperation<'a> {
	key: &'a [u8],
	value: &'a [u8],
}

impl<'a> InsertOperation<'a> {
	fn write(&self, buffer: &mut Vec<u8>, field_body_size: usize, const_value: bool) -> usize {
		let buffer_len = buffer.len();
		append_record(buffer, self.key, self.value, field_body_size, const_value);
		buffer.len() - buffer_len
	}
}

struct DeleteOperation<'a> {
	key: &'a [u8],
}

impl<'a> DeleteOperation<'a> {
	fn write(&self, buffer: &mut Vec<u8>, field_body_size: usize) -> usize {
		unimplemented!();
	}
}

enum OperationWriterState<'op, 'db> {
	ConsumeNextOperation,
	InsertOperation(InsertOperation<'op>, usize),
	OverwriteOperation(InsertOperation<'op>, usize),
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
	buffer: Vec<u8>,
	previous_operation_start: Option<usize>,
	field_body_size: usize,
	prefix_bits: u8,
	const_value: bool,
}

impl<'op, 'db, I: Iterator<Item = Operation<'op>>> OperationWriter<'op, 'db, I> {
	/// Creates new operations writer. All operations needs to be ordered by key.
	pub fn new(operations: I, database: &'db [u8], field_body_size: usize, prefix_bits: u8, const_value: bool) -> Self {
		OperationWriter {
			state: OperationWriterState::ConsumeNextOperation,
			operations: operations.peekable(),
			spaces: SpaceIterator::new(database, field_body_size, 0),
			buffer: Vec::new(),
			previous_operation_start: None,
			field_body_size,
			prefix_bits,
			const_value,
		}
	}

	fn step(&mut self) -> Result<OperationWriterStep> {
		let next_state = match self.state {
			OperationWriterState::ConsumeNextOperation => {
				// write the len of previous operation
				if let Some(operation_start) = self.previous_operation_start {
					let len = self.buffer.len() - (operation_start + 12);
					LittleEndian::write_u32(&mut self.buffer[operation_start + 8..operation_start + 12], len as u32);
				}

				// get next operation
				match self.operations.next() {
					// if there is no operation, finish
					None => OperationWriterState::Finished,
					Some(operation) => match operation {
						// if it's insert, peek and move to space offset until you find insertion place
						Operation::Insert(key, value) => {
							let (offset, overwrite) = loop {
								let k = Key::new(key, self.prefix_bits);
								self.spaces.move_offset_forward(k.offset(self.field_body_size));

								let space = self.spaces.peek()
									.expect("TODO: db, end0")
									.as_ref().expect("TODO: malformed DB")
									.clone();

								match space {
									Space::Empty(ref space) => {
										break (space.offset, false);
									},
									Space::Occupied(ref space) => {
										match Record::extract_key(&space.data, self.field_body_size, key.len()).partial_cmp(&key).unwrap() {
											cmp::Ordering::Less => {
												// seek
												let _ = self.spaces.next();
											},
											cmp::Ordering::Equal => {
												// overwrite
												let _ = self.spaces.next();
												// TODO: handle edge case when new record len != old record len
												break (space.offset, true);
											},
											cmp::Ordering::Greater => {
												// insert then write old record
												break (space.offset, false);
											}
										}
									}
								}
							};

							// write previous operation start
							self.previous_operation_start = Some(self.buffer.len());
							self.buffer.write_u64::<LittleEndian>(offset as u64).unwrap();
							// reserve space for len
							self.buffer.extend_from_slice(&[0; 4]);

							let insert = InsertOperation {
								key,
								value,
							};

							// call insert operation
							if overwrite {
								OperationWriterState::OverwriteOperation(insert, 0)
							} else {
								OperationWriterState::InsertOperation(insert, 0)
							}
						},
						Operation::Delete(key) => {
							// if it's delete peek and move to delete place
							// if not found, move to next operation
							unimplemented!();
						},
					}
				}
			},
			OperationWriterState::InsertOperation(ref operation, len) => {
				// write next operation to a buffer
				let written = operation.write(&mut self.buffer, self.field_body_size, self.const_value);
				// increase the len size by operation size
				// advance to the next operation
				OperationWriterState::Advance(len + written)
			},
			OperationWriterState::OverwriteOperation(ref operation, len) => {
				// write overwrite operation to a buffer
				let _ = operation.write(&mut self.buffer, self.field_body_size, self.const_value);
				// len should not be increased
				// advance to the next operation
				OperationWriterState::Advance(len)
			},
			OperationWriterState::DeleteOperation(ref operation, len) => {
				// write to buffer deleted fields
				// len should not be increased
				// advance to the next operation
				OperationWriterState::Advance(len)
			},
			OperationWriterState::Advance(len) => {
				// peek next space and operation and decide which one should go first
				let space = self.spaces.peek()
					.expect("TODO: db, end1")
					.as_ref().expect("TODO: malformed DB")
					.clone();

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
								match Record::extract_key(&space.data, self.field_body_size, key.len()).partial_cmp(&key).unwrap() {
									// existing record is smaller
									cmp::Ordering::Less => {
										// remove his space
										let _ = self.spaces.next();
										OperationWriterState::OccupiedSpace(space, len)
									}
									// replace records
									cmp::Ordering::Equal => {
										let _ = self.spaces.next();
										let _ = self.operations.next();
										let insert = InsertOperation {
											key,
											value,
										};
										// TODO: handle edge case when new record len != old record len
										OperationWriterState::OverwriteOperation(insert, len)
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
								// TODO: if delete operation call delete
								unimplemented!();
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
				self.buffer.extend_from_slice(space.data);
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
		Ok(self.buffer)
	}
}
