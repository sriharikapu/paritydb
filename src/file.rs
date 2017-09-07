use std::path::{Path, PathBuf};
use std::fs::OpenOptions;
use std::marker;
use byteorder::{ByteOrder, LittleEndian};
use memmap::{Mmap, MmapViewSync, Protection};
use error::Result;

pub struct File<K, V> {
	path: PathBuf,
	len_view: MmapViewSync,
	view: MmapViewSync,
	key_marker: marker::PhantomData<K>,
	value_marker: marker::PhantomData<V>,
}

impl<K, V> File<K, V> {
	/// Arbitrary selected 268MB allocated for each slice of the database.
	const ALLOC_SIZE: u64 = 0xf_ff_ff_ff;

	/// Reallocation threshold, roughly 16MB.
	const REALLOC_SIZE: u64 = 0xff_ff_ff;

	pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
		Self::open_with(path, None)
	}

	fn open_with<P: AsRef<Path>>(path: P, additional_space: Option<u64>) -> Result<Self> {
		let file = OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.open(path.as_ref())?;

		let meta = file.metadata()?;
		let len = match additional_space {
			Some(len) => meta.len() + len,
			None => match meta.len() {
				0 => Self::ALLOC_SIZE,
				len => len,
			}
		};

		file.set_len(len);

		let view = Mmap::open(&file, Protection::ReadWrite)?.into_view_sync();
		let (mut len_view, mut view) = view.split_at(8)?;
		len_view.restrict(0, 8)?;
		let view_len = view.len();
		view.restrict(0, view_len)?;

		let result = File {
			path: path.as_ref().to_path_buf(),
			len_view,
			view,
			key_marker: marker::PhantomData,
			value_marker: marker::PhantomData,
		};

		Ok(result)
	}

	pub fn len(&self) -> u64 {
		LittleEndian::read_u64(unsafe { self.len_view.as_slice() })
	}

	pub fn set_len(&mut self, len: u64) {
		LittleEndian::write_u64(unsafe { self.len_view.as_mut_slice() }, len);
	}

	pub fn needs_realloc(used: u64, allocated: u64) -> bool {
		allocated - used <= Self::REALLOC_SIZE
	}

	pub fn realloc_if_needed(self) -> Result<Self> {
		if Self::needs_realloc(self.len(), self.view.len() as u64 + 8) {
			Self::open_with(self.path, Some(Self::ALLOC_SIZE))
		} else {
			Ok(self)
		}
	}

	pub fn insert(_key: K, _value: V) -> u64 {
		unimplemented!();
	}
}

#[cfg(test)]
mod tests {
	extern crate tempdir;		
	use self::tempdir::TempDir;
	use super::File;

	#[test]
	fn test_needs_realloc() {
		fn needs(size: u64) -> bool {
			File::<[u8; 0], [u8; 0]>::needs_realloc(size, 0xf_ff_ff_ff)
		}

		let tempdir = TempDir::new("test_needs_realloc").unwrap();
		assert!(!needs(0));
		assert!(!needs(1));
		assert!(!needs(0xff_ff_ff));
		assert!(needs(0xf_ff_ff_ff));
		assert!(needs(0xf_ff_ff_ff - 0xff_ff_ff));
		assert!(!needs(0xf_ff_ff_ff - 0xff_ff_ff - 1));
	}
}
