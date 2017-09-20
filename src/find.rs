use field::iterator::FieldIterator;
use field::{Error, Header, HEADER_SIZE};
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
	let iter = FieldIterator::new(data, field_body_size)?;

	for (index, field) in iter.enumerate() {
		match field.header()? {
			Header::Uninitialized => return Ok(RecordResult::NotFound),
			Header::Inserted => {
				let offset = (field_body_size + HEADER_SIZE) * index;
				if Record::extract_key(&data[offset..], field_body_size, key.len()) == key {
					let record = Record::new(&data[offset..], field_body_size, value_size, key.len());
					return Ok(RecordResult::Found(record));
				}
			},
			Header::Continued => {},
			Header::Deleted => {
				let offset = (field_body_size + HEADER_SIZE) * index;
				if Record::extract_key(&data[offset..], field_body_size, key.len()) == key {
					return Ok(RecordResult::NotFound);
				}
			}
		}
	}

	Ok(RecordResult::OutOfRange)
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum EmptySpace {
	Found {
		offset: usize,
		size: usize,
	},
	NotFound,
}

pub fn find_empty_space(data: &[u8], field_body_size: usize, space: usize) -> Result<EmptySpace, Error> {
	let iter = FieldIterator::new(data, field_body_size)?;

	let mut result_space = EmptySpace::NotFound;
	for (index, field) in iter.enumerate() {
		result_space = match (field.is_empty()?, result_space) {
			(true, EmptySpace::NotFound) => {
				let new_space = EmptySpace::Found {
					offset: (field_body_size + HEADER_SIZE) * index,
					size: field_body_size,
				};
				if field_body_size >= space {
					return Ok(new_space);
				}
				new_space
			},
			(false, EmptySpace::NotFound) => EmptySpace::NotFound,
			(true, EmptySpace::Found { offset, size }) => {
				let new_size = size + field_body_size;
				let new_space = EmptySpace::Found {
					offset,
					size: new_size,
				};

				if field_body_size >= space {
					return Ok(new_space);
				}
				new_space
			},
			(false, found) => return Ok(found),
		};
	}

	Ok(result_space)
}

#[cfg(test)]
mod tests {
	use super::{find_record, RecordResult};
	use super::{find_empty_space, EmptySpace};
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
	fn test_find_deleted_record_location_for_reading() {
		let value_size = record::ValueSize::Constant(0);
		let body_size = 3;
		let data = [3, 1, 2, 3, 1, 4, 5, 6];
		let key = [1, 2, 3];
		let key2 = [4, 5, 6];
		let location = RecordResult::NotFound;

		assert_eq(location, find_record(&data, body_size, value_size, &key).unwrap());
		expect_record(find_record(&data, body_size, value_size, &key2).unwrap(), &[4, 5, 6], &[]);
	}

	#[test]
	fn test_find_out_of_range_record_location_for_reading() {
		let value_size = record::ValueSize::Constant(0);
		let body_size = 3;
		let data = [1, 1, 2, 3, 1, 4, 5, 6];
		let key = [1, 4, 5];
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

	#[test]
	fn test_find_empty_space() {
		let body_size = 3;
		let data = [0, 1, 2, 3, 1, 4, 5, 6];
		let space = 3;
		let space2 = 4;
		let space3 = 2;
		let location = EmptySpace::Found {
			offset: 0,
			size: 3,
		};

		assert_eq!(location, find_empty_space(&data, body_size, space).unwrap());
		assert_eq!(location, find_empty_space(&data, body_size, space2).unwrap());
		assert_eq!(location, find_empty_space(&data, body_size, space3).unwrap());
	}

	#[test]
	fn test_find_empty_space2() {
		let body_size = 3;
		let data = [0, 1, 2, 3, 3, 4, 5, 6];
		let space = 3;
		let space2 = 4;
		let space3 = 2;
		let location = EmptySpace::Found {
			offset: 0,
			size: 3,
		};
		let location2 = EmptySpace::Found {
			offset: 0,
			size: 6,
		};

		assert_eq!(location, find_empty_space(&data, body_size, space).unwrap());
		assert_eq!(location2, find_empty_space(&data, body_size, space2).unwrap());
		assert_eq!(location, find_empty_space(&data, body_size, space3).unwrap());
	}

	#[test]
	fn test_find_empty_space3() {
		let body_size = 3;
		let data = [1, 1, 2, 3, 2, 4, 5, 6];
		let space = 3;
		let space2 = 4;
		let space3 = 2;
		let location = EmptySpace::NotFound;

		assert_eq!(location, find_empty_space(&data, body_size, space).unwrap());
		assert_eq!(location, find_empty_space(&data, body_size, space2).unwrap());
		assert_eq!(location, find_empty_space(&data, body_size, space3).unwrap());
	}

	#[test]
	fn test_find_empty_space4() {
		let body_size = 3;
		let data = [1, 1, 2, 3, 0, 4, 5, 6];
		let space = 3;
		let space2 = 4;
		let space3 = 2;
		let location = EmptySpace::Found {
			offset: 4,
			size: 3,
		};

		assert_eq!(location, find_empty_space(&data, body_size, space).unwrap());
		assert_eq!(location, find_empty_space(&data, body_size, space2).unwrap());
		assert_eq!(location, find_empty_space(&data, body_size, space3).unwrap());
	}
}
