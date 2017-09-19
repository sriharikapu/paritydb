use field::{Error, Header, HEADER_SIZE};
use field::iterator::FieldIterator;
use record::Record;

/// Record location.
#[derive(Debug, PartialEq)]
pub enum RecordLocationForReading {
	/// Deviation from the begining of the data.
	Offset(usize),
	/// Record does not exist or was deleted.
	NotFound,
	/// Record does no exist in this memory slice, but may in the next one
	OutOfRange,
}

pub fn find_record_location_for_reading(
	data: &[u8],
	record_headers: bool,
	field_body_size: usize,
	key: &[u8],
) -> Result<RecordLocationForReading, Error> {
	assert!(!record_headers, "Variable-len records are not supported yet.");

	let iter = FieldIterator::new(data, field_body_size)?;

	for (index, field) in iter.enumerate() {
		match field.header()? {
			Header::Uninitialized => return Ok(RecordLocationForReading::NotFound),
			Header::Inserted => {
				let offset = (field_body_size + HEADER_SIZE) * index;
				let record = Record::new(&data[offset..], field_body_size, key.len());
				if record.key_is_equal(key) {
					return Ok(RecordLocationForReading::Offset(offset));
				}
			},
			Header::Continued => {},
			Header::Deleted => {
				let offset = (field_body_size + HEADER_SIZE) * index;
				let record = Record::new(&data[offset..], field_body_size, key.len());
				if record.key_is_equal(key) {
					return Ok(RecordLocationForReading::NotFound);
				}
			}
		}
	}

	Ok(RecordLocationForReading::OutOfRange)
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
	use super::{find_record_location_for_reading, RecordLocationForReading};
	use super::{find_empty_space, EmptySpace};

	#[test]
	fn test_find_record_location_for_reading() {
		let body_size = 3;
		let data = [1, 1, 2, 3, 1, 4, 5, 6];
		let key = [1, 2, 3];
		let key2 = [4, 5, 6];
		let location = RecordLocationForReading::Offset(0);
		let location2 = RecordLocationForReading::Offset(4);

		assert_eq!(location, find_record_location_for_reading(&data, false, body_size, &key).unwrap());
		assert_eq!(location2, find_record_location_for_reading(&data, false, body_size, &key2).unwrap());
	}

	#[test]
	fn test_find_deleted_record_location_for_reading() {
		let body_size = 3;
		let data = [3, 1, 2, 3, 1, 4, 5, 6];
		let key = [1, 2, 3];
		let key2 = [4, 5, 6];
		let location = RecordLocationForReading::NotFound;
		let location2 = RecordLocationForReading::Offset(4);

		assert_eq!(location, find_record_location_for_reading(&data, false, body_size, &key).unwrap());
		assert_eq!(location2, find_record_location_for_reading(&data, false, body_size, &key2).unwrap());
	}

	#[test]
	fn test_find_out_of_range_record_location_for_reading() {
		let body_size = 3;
		let data = [1, 1, 2, 3, 1, 4, 5, 6];
		let key = [1, 4, 5];
		let location = RecordLocationForReading::OutOfRange;

		assert_eq!(location, find_record_location_for_reading(&data, false, body_size, &key).unwrap());
	}

	#[test]
	fn test_find_uninitialized_record_location_for_reading() {
		let body_size = 3;
		let data = [0, 1, 2, 3, 1, 4, 5, 6];
		let key = [1, 2, 3];
		let key2 = [4, 5, 6];
		let location = RecordLocationForReading::NotFound;
		let location2 = RecordLocationForReading::NotFound;

		assert_eq!(location, find_record_location_for_reading(&data, false, body_size, &key).unwrap());
		assert_eq!(location2, find_record_location_for_reading(&data, false, body_size, &key2).unwrap());
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
