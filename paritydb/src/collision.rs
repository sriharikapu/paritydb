use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use error::Result;

pub struct Collision {
	prefix: u32,
	file: File,
}

impl Collision {
	fn collision_file_path<P: AsRef<Path>>(path: P, prefix: u32) -> PathBuf {
		let collision_file_name = format!("collision-{}.db", prefix);
		path.as_ref().join(collision_file_name)
	}

	pub fn create<P: AsRef<Path>>(path: P, prefix: u32) -> Result<Collision> {
		// Create directories if necessary.
		fs::create_dir_all(&path)?;

		let collision_file_path = Self::collision_file_path(path, prefix);
		let file = fs::OpenOptions::new()
			.write(true)
			.read(true)
			.create_new(true)
			.open(&collision_file_path)?;

		Ok(Collision { prefix, file })
	}

	pub fn open<P: AsRef<Path>>(path: P, prefix: u32) -> Result<Option<Collision>> {
		let collision_file_path = Self::collision_file_path(path, prefix);
		let open_options = fs::OpenOptions::new()
			.write(true)
			.read(true)
			.open(&collision_file_path);

		let file = match open_options {
			Ok(file) => file,
			Err(ref err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
			Err(err) => return Err(err.into()),
		};

		Ok(Some(Collision { prefix, file }))
	}

	pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
		self.file.seek(SeekFrom::End(0))?;
		self.file.write_u32::<LittleEndian>(key.len() as u32)?;
		self.file.write_all(key)?;
		self.file.write_u32::<LittleEndian>(value.len() as u32)?;
		self.file.write_all(value)?;
		Ok(())
	}

	fn get_aux(&mut self, key: &[u8]) -> io::Result<Vec<u8>> {
		self.file.seek(SeekFrom::Start(0))?;

		loop {
			let key_size = self.file.read_u32::<LittleEndian>()?;
			let mut k = vec![0u8; key_size as usize];
			self.file.read_exact(&mut k)?;
			let value_size = self.file.read_u32::<LittleEndian>()?;

			if k == key {
				let mut value = vec![0u8; value_size as usize];
				self.file.read_exact(&mut value)?;
				return Ok(value);
			} else {
				self.file.seek(SeekFrom::Current(value_size as i64))?;
			}
		}
	}

	pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
		match self.get_aux(key) {
			Ok(res) => Ok(Some(res)),
			Err(ref err) if err.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
			Err(err) => Err(err.into())
		}
	}
}

#[cfg(test)]
mod tests {
	extern crate tempdir;

	use super::Collision;

	#[test]
	fn test_roundtrip() {
		let temp = tempdir::TempDir::new("test_roundtrip").unwrap();

		{
			let mut collision = Collision::create(temp.path(), 0).unwrap();
			collision.put(b"hello", b"world").unwrap();
			assert_eq!(collision.get(b"hello").unwrap().unwrap(), b"world");

		}


		let mut collision = Collision::open(temp.path(), 0).unwrap().unwrap();
		assert_eq!(collision.get(b"hello").unwrap().unwrap(), b"world");
	}
}
