use std::path::{Path, PathBuf};
use std::fs;
use std::io::Write;

use hex_slice::AsHex;
use memmap::{Mmap, Protection};
use tiny_keccak::{sha3_256, Keccak};

use error::{ErrorKind, Result};
use flush::iterator::IdempotentOperationIterator;
use flush::writer::OperationWriter;
use options::InternalOptions;
use transaction::Operation;

/// Stores transaction operations as a set of idempotent operations.
#[derive(Debug)]
pub struct Flush {
	path: PathBuf,
	mmap: Mmap,
}

impl Flush {
	const FILE_NAME: &'static str = "db.flush";
	const CHECKSUM_SIZE: usize = 32;

	/// Creates memmap which is a set of only idempotent operations.
	pub fn new<'a, I, P>(dir: P, options: &InternalOptions, db: &[u8], operations: I) -> Result<Flush>
		where I: IntoIterator<Item = Operation<'a>>, P: AsRef<Path> {

		let writer = OperationWriter::new(
			operations.into_iter(),
			db,
			options.field_body_size,
			options.external.key_index_bits,
			options.external.value_len.is_const(),
		);

		let positive_operations = writer.run()?;

		let path = dir.as_ref().join(Flush::FILE_NAME);

		let mut file = fs::OpenOptions::new()
			.write(true)
			.read(true)
			.create_new(true)
			.open(&path)?;
		file.set_len(positive_operations.len() as u64 + Self::CHECKSUM_SIZE as u64)?;
		file.flush()?;

		let mut mmap = Mmap::open(&file, Protection::ReadWrite)?;
		Keccak::sha3_256(&positive_operations, unsafe { &mut mmap.as_mut_slice()[..Self::CHECKSUM_SIZE] });
		unsafe { &mut mmap.as_mut_slice()[Self::CHECKSUM_SIZE..] }.write_all(&positive_operations)?;
		mmap.flush()?;

		Ok(Flush {
			path,
			mmap,
		})
	}

	/// Open flush file if it exists. It it does not, returns None.
	pub fn open<P: AsRef<Path>>(dir: P) -> Result<Option<Flush>> {
		let path = dir.as_ref().join(Self::FILE_NAME);
		let mmap = Mmap::open_path(&path, Protection::Read)?;
		{
			let checksum = unsafe { &mmap.as_slice()[..Self::CHECKSUM_SIZE] };
			let data = unsafe { &mmap.as_slice()[Self::CHECKSUM_SIZE..] };
			let hash = sha3_256(data);
			if hash != checksum {
				return Err(ErrorKind::CorruptedFlush(
					path,
					format!(
						"Expected: {:02x}, Got: {:02x}",
						hash.as_hex(),
						checksum.as_hex(),
					)
				).into());
			}
		}
		Ok(Some(Flush {
			path,
			mmap,
		}))
	}

	/// Flushes idempotent operations to the database.
	pub fn flush(&self, db: &mut [u8]) {
		let operations = IdempotentOperationIterator::new(unsafe { &self.mmap.as_slice()[Self::CHECKSUM_SIZE..] });

		for o in operations {
			db[o.offset..o.offset + o.data.len()].copy_from_slice(o.data);
		}
	}

	/// Delete flush file. Should be called only after database has been successfully flushed.
	pub fn delete(self) -> Result<()> {
		fs::remove_file(self.path)?;
		Ok(())
	}
}
