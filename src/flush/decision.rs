//! Decision
//!
//! Our database supports two types of operations. Inserts and deletes.
//! This module is responsible for comparing existing records with new operations
//! and making decisions based on the result of this comparison. The decision is
//! later used to create idempotent database operation.

use std::cmp;
use record::Record;
use space::Space;
use transaction::Operation;

/// Decision made after comparing existing record and new operation.
#[derive(Debug)]
pub enum Decision<'o, 'db> {
	/// Returns when operation is an insert operation and it's key is lower
	/// then existing record's key or space is empty.
	///
	/// Operation should be marked as inserted.
	/// Operations iterator should be moved to the next value.
	/// Spaces iterator offset should be moved to the next operation location.
	InsertOperationBeforeOccupiedSpace {
		key: &'o [u8],
		value: &'o [u8],
		offset: usize,
	},
	InsertOperationIntoEmptySpace {
		key: &'o [u8],
		value: &'o [u8],
		offset: usize,
		space_len: usize,
	},
	/// Returns when operation is an insert operation and it's key is equal
	/// to existing record's key.
	OverwriteOperation {
		key: &'o [u8],
		value: &'o [u8],
		offset: usize,
		old_len: usize,
	},
	/// Returned when operation is an insert operation and the space is marked as deleted.
	///
	/// The operation should be checked with the next space.
	/// If it's `InsertOperation`, it should be written in this space.
	/// If it's `OverwriteOperation`, it should be written in that space.
	/// If it's `InsertIntoDeletedSpace`, it should be checked with the space until different result occurs.
	/// If it's `SeekSpace`, we spaces.iterator offset should be moved to this operation location.
	/// All other cases are unreachable.
	InsertIntoDeletedSpace {
		key: &'o [u8],
		value: &'o [u8],
		offset: usize,
	},
	/// Returns when operation is a delete operation and it's key is equal
	/// to existing record's key.
	///
	/// The record should be marked as deleted.
	DeleteOperation {
		offset: usize,
		len: usize,
	},
	/// Space is occupied and existing record's key is greater then operation's key.
	/// No decision could be made.
	///
	/// If it's occupied space, it should be appended to current idempotent operation.
	SeekSpace,
	/// Returned only on delete, when deleted value is not found in the database.
	///
	/// Operations iterator should be moved to the next value.
	/// Spaces iterator offset should be moved to next operation location.
	IgnoreOperation,
	ConsumeEmptySpace {
		len: usize,
	},
	RewriteOccupiedSpace {
		data: &'db [u8],
	},
}

/// Compares occupied space data and operation key.
#[inline]
fn compare_space_and_operation(space: &[u8], key: &[u8], field_body_size: usize) -> cmp::Ordering {
	Record::extract_key(space, field_body_size, key.len()).partial_cmp(&key).unwrap()
}

pub fn decision<'o, 'db>(operation: Operation<'o>, space: Space<'db>, is_new: bool, field_body_size: usize) -> Decision<'o, 'db> {
	match (operation, space, is_new) {
		(Operation::Insert(key, value), Space::Empty(space), true) => Decision::InsertOperationIntoEmptySpace {
			key,
			value,
			offset: space.offset,
			space_len: space.len,
		},
		(Operation::Insert(_, _), Space::Empty(space), false) => Decision::ConsumeEmptySpace {
			len: space.len,
		},
		(Operation::Insert(key, value), Space::Deleted(space), _) => Decision::InsertIntoDeletedSpace {
			key,
			value,
			offset: space.offset,
		},
		(Operation::Insert(key, value), Space::Occupied(space), _) => {
			match compare_space_and_operation(space.data, key, field_body_size) {
				cmp::Ordering::Less if is_new => Decision::SeekSpace,
				cmp::Ordering::Less => Decision::RewriteOccupiedSpace {
					data: space.data,
				},
				cmp::Ordering::Equal => Decision::OverwriteOperation {
					key,
					value,
					offset: space.offset,
					old_len: space.data.len()
				},
				cmp::Ordering::Greater => Decision::InsertOperationBeforeOccupiedSpace {
					key,
					value,
					offset: space.offset,
				}
			}
		},
		// record does not exist
		(Operation::Delete(_), Space::Empty(_), true) => Decision::IgnoreOperation,
		(Operation::Delete(_), Space::Empty(space), false) => Decision::ConsumeEmptySpace {
			len: space.len,
		},
		// we know nothing about this space yet
		(Operation::Delete(_), Space::Deleted(space), true) => Decision::SeekSpace,
		(Operation::Delete(_), Space::Deleted(space), false) => Decision::RewriteOccupiedSpace {
			data: space.data,
		},
		(Operation::Delete(key), Space::Occupied(space), _) => {
			match compare_space_and_operation(space.data, key, field_body_size) {
				cmp::Ordering::Less if is_new => Decision::SeekSpace,
				cmp::Ordering::Less => Decision::RewriteOccupiedSpace {
					data: space.data,
				},
				cmp::Ordering::Equal => Decision::DeleteOperation {
					offset: space.offset,
					len: space.data.len(),
				},
				// record does not exist
				cmp::Ordering::Greater => Decision::IgnoreOperation,
			}
		},
	}
}
