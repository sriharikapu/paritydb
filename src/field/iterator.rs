use field::error::{Error, ErrorKind};
use field::field_size;
use field::header::Header;

pub struct Field<'a> {
	data: &'a [u8],
}

impl<'a> From<&'a [u8]> for Field<'a> {
	fn from(data: &'a [u8]) -> Self {
		Field {
			data,
		}
	}
}

impl<'a> Field<'a> {
	pub fn header(&self) -> Result<Header, Error> {
		if self.data.is_empty() {
			return Err(ErrorKind::InvalidLength.into());
		}

		Ok(Header::from_u8(self.data[0]).ok_or(ErrorKind::InvalidHeader)?)
	}
}

#[derive(Clone)]
pub struct FieldIterator<'a> {
	data: &'a [u8],
	field_size: usize,
}

impl<'a> FieldIterator<'a> {
	pub fn new(data: &'a [u8], field_body_size: usize) -> Result<Self, Error> {
		let field_size = field_size(field_body_size);
		if (data.len() % field_size) != 0 {
			return Err(ErrorKind::InvalidLength.into());
		}

		Ok(FieldIterator {
			data,
			field_size,
		})
	}
}

impl<'a> Iterator for FieldIterator<'a> {
	type Item = Field<'a>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.data.is_empty() {
			return None;
		}

		let (next_field, new_data) = self.data.split_at(self.field_size);
		self.data = new_data;
		Some(next_field.into())
	}
}
