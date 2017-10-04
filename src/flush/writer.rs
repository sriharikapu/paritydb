//! Flush operations writer

use std::iter::Peekable;

use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};

use error::Result;
use flush::decision::{decision, Decision};
use key::Key;
use metadata::Metadata;
use record::{append_record, append_deleted};
use space::{SpaceIterator, Space};
use transaction::Operation;

#[inline]
fn write_insert_operation(buffer: &mut Vec<u8>, key: &[u8], value: &[u8], field_body_size: usize, const_value: bool) -> usize {
	let buffer_len = buffer.len();
	append_record(buffer, key, value, field_body_size, const_value);
	buffer.len() - buffer_len
}

#[inline]
fn overwrite_operation(buffer: &mut Vec<u8>, key: &[u8], value: &[u8], field_body_size: usize, const_value: bool, old_len: usize) -> usize {
	let buffer_len = buffer.len();
	append_record(buffer, key, value, field_body_size, const_value);
	let written = buffer.len() - buffer_len;
	if written < old_len {
		let deleted = old_len - written;
		append_deleted(buffer, deleted, field_body_size);
	}
	buffer.len() - buffer_len
}

#[inline]
fn write_delete_operation(buffer: &mut Vec<u8>, len: usize, field_body_size: usize) {
	append_deleted(buffer, len, field_body_size);
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
		if self.denoted_operation_start.is_none() {
			self.denoted_operation_start = Some(self.inner.len());
			self.inner.write_u64::<LittleEndian>(offset).unwrap();
			// reserve space for len
			self.inner.extend_from_slice(&[0; 4]);
		}
	}

	#[inline]
	fn finish_operation(&mut self) {
		if let Some(operation_start) = self.denoted_operation_start.take() {
			let len = self.inner.len() - (operation_start + 12);
			LittleEndian::write_u32(&mut self.inner[operation_start + 8..operation_start + 12], len as u32);
		}
	}

	#[inline]
	fn is_finished(&self) -> bool {
		self.denoted_operation_start.is_none()
	}
}

enum OperationWriterStep {
	Stepped,
	Finished
}

/// Writes transactions as a set of idempotent operations
pub struct OperationWriter<'db, I: Iterator> {
	operations: Peekable<I>,
	spaces: SpaceIterator<'db>,
	metadata: &'db mut Metadata,
	buffer: OperationBuffer,
	field_body_size: usize,
	prefix_bits: u8,
	const_value: bool,
	empty_bytes_debt: usize,
}

impl<'op, 'db, I: Iterator<Item = Operation<'op>>> OperationWriter<'db, I> {
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
			operations: operations.peekable(),
			spaces: SpaceIterator::new(database, field_body_size, 0),
			metadata,
			buffer: OperationBuffer::default(),
			field_body_size,
			prefix_bits,
			const_value,
			empty_bytes_debt: 0,
		}
	}

	fn step(&mut self) -> Result<OperationWriterStep> {
		let operation = match self.operations.peek() {
			Some(operation) => operation.clone(),
			None => {
				// loop until the transaction is finished
				while self.empty_bytes_debt != 0 {
					let space = self.spaces.next().expect("TODO: db end")?;
					match space {
						Space::Empty(space) => {
							self.empty_bytes_debt -= space.len;
						},
						Space::Deleted(space) => {
							// write it to a buffer if we are in 'rewrite' state
							self.buffer.as_raw_mut().extend_from_slice(space.data);
						},
						Space::Occupied(space) => {
							// write it to a buffer if we are in 'rewrite' state
							self.buffer.as_raw_mut().extend_from_slice(space.data);
						},
					}
				}
				// write the len of previous operation
				self.buffer.finish_operation();
				return Ok(OperationWriterStep::Finished)
			}
		};

		// if self.empty_bytes_debt != 0, we are in a different state, when a priority is consume spaces
		let prefixed_key = Key::new(operation.key(), self.prefix_bits);
		if self.empty_bytes_debt == 0 {
			// write the len of previous operation
			self.buffer.finish_operation();
			self.spaces.move_offset_forward(prefixed_key.offset(self.field_body_size));
		}

		assert_eq!(self.empty_bytes_debt == 0, self.buffer.is_finished());

		let space = self.spaces.peek().expect("TODO: db end?")?;
		let d = decision(operation, space, self.buffer.is_finished(), self.field_body_size);
		println!("d: {:?}", d);
		match d {
			Decision::InsertOperationIntoEmptySpace { key, value, offset, space_len } => {
				// advance iterators
				let _ = self.operations.next();
				let _ = self.spaces.next();

				// denote operation start
				self.buffer.denote_operation_start(offset as u64);
				let written = write_insert_operation(self.buffer.as_raw_mut(), key, value, self.field_body_size, self.const_value);
				// space has been consumed
				self.empty_bytes_debt += written - space_len;
				self.metadata.insert_record(prefixed_key.prefix, written);
			},
			Decision::InsertOperationBeforeOccupiedSpace { key, value, offset } => {
				// advance iterators
				let _ = self.operations.next();

				// denote operation start
				self.buffer.denote_operation_start(offset as u64);
				let written = write_insert_operation(self.buffer.as_raw_mut(), key, value, self.field_body_size, self.const_value);
				self.empty_bytes_debt += written;
				self.metadata.insert_record(prefixed_key.prefix, written);
			},
			Decision::OverwriteOperation { key, value, offset, old_len } => {
				// advance iterators
				let _ = self.operations.next();
				let _ = self.spaces.next();

				// denote operation start
				self.buffer.denote_operation_start(offset as u64);
				let written = overwrite_operation(self.buffer.as_raw_mut(), key, value, self.field_body_size, self.const_value, old_len);
				assert!(written >= old_len, "old record has not been overwritten");
				self.empty_bytes_debt += written - old_len;
				// update metadata
				self.metadata.update_record_len(old_len, written);
			},
			Decision::InsertIntoDeletedSpace { .. } => {
				let _ = self.spaces.next();
				// TODO:
				unimplemented!();
			},
			Decision::SeekSpace => {
				// advance iterator
				let _ = self.spaces.next();
			},
			Decision::IgnoreOperation => {
				// ignore this operation
				let _ = self.operations.next();
			},
			Decision::ConsumeEmptySpace { len } => {
				let _ = self.spaces.next();
				self.empty_bytes_debt -= len;
			},
			Decision::RewriteOccupiedSpace { data } => {
				// advance space iterator
				let _ = self.spaces.next();
				// rewrite the space to a buffer
				self.buffer.as_raw_mut().extend_from_slice(data);
			},
			Decision::DeleteOperation { offset, len } => {
				// advance iterators
				let _ = self.operations.next();
				let _ = self.spaces.next();

				// denote operation start
				self.buffer.denote_operation_start(offset as u64);
				write_delete_operation(self.buffer.as_raw_mut(), len, self.field_body_size);
			},
		}
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
