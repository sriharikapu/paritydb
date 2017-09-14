use field::{FieldIterator, Error, Header};
use record::Record;

/// Record location.
#[derive(Debug, PartialEq)]
pub enum RecordLocationForReading {
	/// Deviation from the begining of the data.
	Offset(u32),
	/// Record does not exist or was deleted.
	NotFound,
	/// Record does no exist in this memory slice, but may in the next one
	OutOfRange,
}

pub fn find_record_location_for_reading(data: &[u8], field_body_size: usize, key: &[u8]) -> Result<RecordLocationForReading, Error> {
	let iter = FieldIterator::new(data, field_body_size)?;

	for (index, field) in iter.enumerate() {
		match field.header()? {
			Header::Uninitialized => return Ok(RecordLocationForReading::NotFound),
			Header::Insert => {
				let offset = (field_body_size + 1) * index;
				let record = Record::new(&data[offset..], field_body_size, key.len());
				if record.key_is_equal(key) {
					return Ok(RecordLocationForReading::Offset(offset as u32));
				}
			},
			Header::Continuation => {},
			Header::Deleted => {
				let offset = (field_body_size + 1) * index;
				let record = Record::new(&data[offset..], field_body_size, key.len());
				if record.key_is_equal(key) {
					return Ok(RecordLocationForReading::NotFound);
				}
			}
		}
	}

	Ok(RecordLocationForReading::OutOfRange)
}

#[cfg(test)]
mod tests {
	use super::{find_record_location_for_reading, RecordLocationForReading};

	#[test]
	fn test_find_record_location_for_reading() {
		let body_size = 3;
		let data = [1, 1, 2, 3, 1, 4, 5, 6];
		let key = [1, 2, 3];
		let key2 = [4, 5, 6];
		let location = RecordLocationForReading::Offset(0);
		let location2 = RecordLocationForReading::Offset(4);

		assert_eq!(location, find_record_location_for_reading(&data, body_size, &key).unwrap());
		assert_eq!(location2, find_record_location_for_reading(&data, body_size, &key2).unwrap());
	}

	#[test]
	fn test_find_deleted_record_location_for_reading() {
		let body_size = 3;
		let data = [3, 1, 2, 3, 1, 4, 5, 6];
		let key = [1, 2, 3];
		let key2 = [4, 5, 6];
		let location = RecordLocationForReading::NotFound;
		let location2 = RecordLocationForReading::Offset(4);

		assert_eq!(location, find_record_location_for_reading(&data, body_size, &key).unwrap());
		assert_eq!(location2, find_record_location_for_reading(&data, body_size, &key2).unwrap());
	}

	#[test]
	fn test_find_out_of_range_record_location_for_reading() {
		let body_size = 3;
		let data = [1, 1, 2, 3, 1, 4, 5, 6];
		let key = [1, 4, 5];
		let location = RecordLocationForReading::OutOfRange;

		assert_eq!(location, find_record_location_for_reading(&data, body_size, &key).unwrap());
	}

	#[test]
	fn test_find_uninitialized_record_location_for_reading() {
		let body_size = 3;
		let data = [0, 1, 2, 3, 1, 4, 5, 6];
		let key = [1, 2, 3];
		let key2 = [4, 5, 6];
		let location = RecordLocationForReading::NotFound;
		let location2 = RecordLocationForReading::NotFound;

		assert_eq!(location, find_record_location_for_reading(&data, body_size, &key).unwrap());
		assert_eq!(location2, find_record_location_for_reading(&data, body_size, &key2).unwrap());
	}
}
