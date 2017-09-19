use std::ops::Deref;
use error::Result;
use field::HEADER_SIZE;

#[derive(Debug, PartialEq)]
pub enum ValuesLen {
	Constant(usize),
	Variable { average: usize },
}

impl ValuesLen {
	pub(crate) fn size(&self) -> usize {
		match *self {
			ValuesLen::Constant(x) => x,
			ValuesLen::Variable { average } => average,
		}
	}

	pub(crate) fn is_variable(&self) -> bool {
		if let ValuesLen::Variable { .. } = *self {
			true
		} else {
			false
		}
	}
}

#[derive(Debug, PartialEq)]
pub struct Options {
	/// Number of eras to keep in the journal.
	pub journal_eras: usize,
	/// The DB will re-allocate to twice as big size in case there is more
	/// than `extend_threshold_percent` occupied entries.
	pub extend_threshold_percent: u8,
	/// Number of bits from the key used to create search index.
	pub key_index_bits: u8,
	/// Key length in bytes.
	pub key_len: usize,
	/// Value length in bytes.
	pub value_len: ValuesLen,
}

impl Default for Options {
	fn default() -> Self {
		Options {
			journal_eras: 5,
			extend_threshold_percent: 80,
			key_index_bits: 8,
			key_len: 32,
			value_len: ValuesLen::Constant(64),
		}
	}
}

impl Options {
	pub fn with<F>(f: F) -> Self where
		F: FnOnce(&mut Self),
	{
		let mut options = Options::default();
		f(&mut options);
		options
	}
}

#[derive(Debug, PartialEq)]
pub(crate) struct InternalOptions {
	pub external: Options,
	pub field_body_size: usize,
	pub initial_db_size: u64,
}

impl Deref for InternalOptions {
	type Target = Options;

	fn deref(&self) -> &Self::Target {
		&self.external
	}
}

impl InternalOptions {
	pub fn from_external(external: Options) -> Result<Self> {
		if external.extend_threshold_percent > 100 {
			// TODO [ToDr] Return proper errors here.
			panic!("Extend threshold percent cannot be greater than 100.");
		}
		if (external.key_index_bits as usize + 7) / 8 >  external.key_len {
			panic!("key_index_bits too large");
		}

		let field_body_size = external.key_len + external.value_len.size();
		let initial_db_size = (2u64 << external.key_index_bits) * (field_body_size as u64 + HEADER_SIZE as u64);

		Ok(InternalOptions {
			external,
			field_body_size,
			initial_db_size,
		})
	}
}
