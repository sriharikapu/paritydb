use std::cmp;

use field::iterator::FieldHeaderIterator;
use field::{Error, Header, field_size};
use record::{ValueSize, Record};

/// Record location.
#[derive(Debug)]
pub enum RecordResult<'a> {
	/// Found existing record
	Found(Record<'a>),
	/// Record does not exist or was deleted.
	NotFound,
	/// Record does no exist in this memory slice, but may in the next one
	OutOfRange,
}

pub fn find_record<'a>(
	data: &'a [u8],
	field_body_size: usize,
	value_size: ValueSize,
	key: &[u8],
) -> Result<RecordResult<'a>, Error> {
	let iter = FieldHeaderIterator::new(data, field_body_size)?;

	let field_size = field_size(field_body_size);
	let mut offset = 0;
	for header in iter {
		let header = header?;
		match header {
			Header::Uninitialized => return Ok(RecordResult::NotFound),
			Header::Inserted => {
				let slice = &data[offset..];
				match Record::extract_key(slice, field_body_size, key.len()).partial_cmp(&key).unwrap() {
					cmp::Ordering::Less => {},
					cmp::Ordering::Equal => {
						let record = Record::new(slice, field_body_size, value_size, key.len());
						return Ok(RecordResult::Found(record));
					},
					cmp::Ordering::Greater => return Ok(RecordResult::NotFound),
				}
			},
			Header::Continued => {},
		}
		offset += field_size;
	}
	Ok(RecordResult::OutOfRange)
}

#[cfg(test)]
mod tests {
	use super::{find_record, RecordResult};
	use record;

	fn expect_record(a: RecordResult, key: &[u8], value: &[u8]) {
		if let RecordResult::Found(record) = a {
			let mut k = Vec::new();
			k.resize(key.len(), 0);
			record.read_key(&mut k);
			assert!(record.key_is_equal(key), "Invalid key. Expected: {:?}, got: {:?}", key, k);

			let mut v = Vec::new();
			v.resize(value.len(), 0);
			record.read_value(&mut v);
			assert!(&*v == value, "Invalid value. Expected: {:?}, got: {:?}", value, v);
		} else {
			assert!(false, "Expected to find record, got: {:?}", a);
		}
	}

	fn assert_eq(a: RecordResult, b: RecordResult) {
		match (a, b) {
			(RecordResult::NotFound, RecordResult::NotFound) => return,
			(RecordResult::OutOfRange, RecordResult::OutOfRange) => return,
			(RecordResult::Found(_), RecordResult::Found(_)) => unimplemented!(),
			(a, b) => {
				assert!(false, "Invalid record result. Expected: {:?}, got: {:?}", a, b);
			}
		}
	}

	#[test]
	fn test_find_record() {
		let value_size = record::ValueSize::Constant(0);
		let body_size = 3;
		let data = [1, 1, 2, 3, 1, 4, 5, 6];
		let key = [1, 2, 3];
		let key2 = [4, 5, 6];

		expect_record(find_record(&data, body_size, value_size, &key).unwrap(), &[1, 2, 3], &[]);
		expect_record(find_record(&data, body_size, value_size, &key2).unwrap(), &[4, 5, 6], &[]);
	}

	#[test]
	fn test_find_not_found_record_location_for_reading() {
		let value_size = record::ValueSize::Constant(0);
		let body_size = 3;
		let data = [1, 1, 2, 3, 1, 4, 5, 6];
		let key = [1, 4, 5];
		let location = RecordResult::NotFound;

		assert_eq(location, find_record(&data, body_size, value_size, &key).unwrap());
	}

	#[test]
	fn test_find_out_of_range_record_location_for_reading() {
		let value_size = record::ValueSize::Constant(0);
		let body_size = 3;
		let data = [1, 1, 2, 3, 1, 4, 5, 6];
		let key = [4, 5, 7];
		let location = RecordResult::OutOfRange;

		assert_eq(location, find_record(&data, body_size, value_size, &key).unwrap());
	}

	#[test]
	fn test_find_uninitialized_record_location_for_reading() {
		let value_size = record::ValueSize::Constant(0);
		let body_size = 3;
		let data = [0, 1, 2, 3, 1, 4, 5, 6];
		let key = [1, 2, 3];
		let key2 = [4, 5, 6];
		let location = RecordResult::NotFound;
		let location2 = RecordResult::NotFound;

		assert_eq(location, find_record(&data, body_size, value_size, &key).unwrap());
		assert_eq(location2, find_record(&data, body_size, value_size, &key2).unwrap());
	}
}
