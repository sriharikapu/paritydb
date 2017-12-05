use std::fs;
use std::io::Write;
use std::path::Path;

use memmap::{Mmap, Protection};

use error::Result;
use prefix_tree::PrefixTree;

#[derive(Debug)]
pub struct MetadataFile {
	pub metadata: Metadata,
	pub mmap: Mmap,
}

impl MetadataFile {
	const META_FILE: &'static str = "meta.db";

	pub fn create<P: AsRef<Path>>(path: P, prefix_bits: u8) -> Result<Self> {
		let meta_file_path = path.as_ref().join(Self::META_FILE);
		let mut file = fs::OpenOptions::new()
			.write(true)
			.create_new(true)
			.open(&meta_file_path)?;
		file.set_len(bytes::len(prefix_bits) as u64)?;
		file.flush()?;

		Self::open(path, prefix_bits)
	}

	pub fn open<P: AsRef<Path>>(path: P, prefix_bits: u8) -> Result<Self> {
		let meta_file_path = path.as_ref().join(Self::META_FILE);
		let mmap = Mmap::open_path(meta_file_path, Protection::ReadWrite)?;
		let metadata = bytes::read(unsafe { mmap.as_slice() }, prefix_bits);

		Ok(MetadataFile { metadata, mmap })
	}

	pub fn update(&mut self, raw_metadata: &[u8], metadata: Metadata) -> Result<()> {
		{
			let meta = unsafe { self.mmap.as_mut_slice() };
			meta.copy_from_slice(raw_metadata);
		}

		self.mmap.flush()?;
		self.metadata = metadata;

		Ok(())
	}
}

/// A structure holding database metadata information.
///
/// Currently we store a prefix tree for fast lookups and iterations
/// and number of bytes occupied by records for determining if
/// key prefix should be increased.
#[derive(Debug, Clone)]
pub struct Metadata {
	/// Database version
	pub db_version: u16,
	/// Number of bytes occupied by records
	/// NOTE: it does not include field headers!
	pub occupied_bytes: u64,
	/// Prefix tree
	pub prefixes: PrefixTree,
}

impl Metadata {
	pub const DB_VERSION: u16 = 0;

	/// Notify that record was inserted.
	pub fn insert_record(&mut self, prefix: u32, len: usize) {
		self.occupied_bytes += len as u64;
		self.prefixes.insert(prefix);
	}

	/// Notify that record was removed.
	///
	/// We can't simply remove prefix from db, cause there might be
	/// more records with the same prefix in the database.
	pub fn remove_record(&mut self, len: usize) {
		self.occupied_bytes -= len as u64;
	}

	/// Notify that record was overwritten.
	pub fn update_record_len(&mut self, old_len: usize, new_len: usize) {
		self.occupied_bytes -= old_len as u64;
		self.occupied_bytes += new_len as u64;
	}

	/// Returns bytes representation of `Metadata`.
	pub fn as_bytes(&self) -> bytes::Metadata {
		bytes::Metadata::new(self)
	}
}

/// Metadata bytes manipulations.
pub mod bytes {
	use byteorder::{LittleEndian, ByteOrder};

	use prefix_tree::PrefixTree;

	/// Bytes representation of `Metadata`.
	pub struct Metadata<'a> {
		metadata: &'a super::Metadata,
	}

	impl<'a> Metadata<'a> {
		const VERSION_SIZE: usize = 2;
		const OCCUPIED_SIZE: usize = 8;

		/// Create new.
		pub fn new(metadata: &'a super::Metadata) -> Self {
			Metadata { metadata }
		}

		/// Copy bytes to given slice.
		/// Panics if the length are not matching.
		pub fn copy_to_slice(&self, data: &mut [u8]) {
			let leaves = self.metadata.prefixes.leaves();
			data[leaves_offset()..].copy_from_slice(leaves);
			LittleEndian::write_u16(data, self.metadata.db_version);
			LittleEndian::write_u64(&mut data[Self::VERSION_SIZE..], self.metadata.occupied_bytes);
		}

		/// Return bytes length of the `Metadata`.
		pub fn len(&self) -> usize {
			len(self.metadata.prefixes.prefix_bits())
		}
	}

	#[inline]
	pub fn leaves_offset() -> usize {
		Metadata::VERSION_SIZE + Metadata::OCCUPIED_SIZE
	}

	/// Returns expected `Metadata` bytes len given prefix bits.
	pub fn len(prefix_bits: u8) -> usize {
		leaves_offset() + PrefixTree::leaf_data_len(prefix_bits)
	}

	/// Read `Metadata` from given slice.
	pub fn read(data: &[u8], prefix_bits: u8) -> super::Metadata {
		let db_version = LittleEndian::read_u16(&data[..Metadata::VERSION_SIZE]);
		let occupied_bytes = LittleEndian::read_u64(&data[Metadata::VERSION_SIZE..]);
		let prefixes = PrefixTree::from_leaves(&data[leaves_offset()..], prefix_bits);

		assert_eq!(db_version, super::Metadata::DB_VERSION);

		super::Metadata {
			db_version,
			occupied_bytes,
			prefixes,
		}
	}
}
