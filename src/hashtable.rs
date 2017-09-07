use std::fs::OpenOptions;
use std::path::Path;
use std::mem;
use byteorder::{LittleEndian, ByteOrder};
use memmap::{Mmap, MmapViewSync, Protection};
use error::Result;
use DATABASE_SLICES;

pub struct Hashtable {
	mmap: Mmap,
}

pub struct HashtableSlice {
	view: MmapViewSync,
}

pub struct Query<'a, T: 'a> {
	inner: &'a T,
}

pub struct SliceQuery(usize);

impl<'a, T: AsRef<[u8]>> Query<'a, T> {
	#[inline]
	pub fn new(inner: &'a T) -> Result<Self> {
		if inner.as_ref().len() <= 3 {
			return Err("Query needs to be at least 3 bytes long".into());
		}
		let query = Query {
			inner,
		};

		Ok(query)
	}

	#[inline]
	pub fn slice_index(&self) -> usize {
		let inner = self.inner.as_ref();
		(inner[0] & 0xf0) as usize
	}

	#[inline]
	pub fn slice_query(&self) -> SliceQuery {
		let inner = self.inner.as_ref();
		let query = (((inner[0] & 0x0f) as usize) << 16) +
			((inner[1] as usize) << 8) +
			(inner[2] as usize);
		SliceQuery(query)
	}
}

impl Hashtable {
	/// The size of 0xffffff (roughly 16M) guaraneets very few collisions
	/// with a reasonable disk usage of 134MB (0xffffff * 8 bytes)
	const SIZE: u64 = 0xff_ff_ff;

	/// Size of hashtable file on disk
	const FILE_SIZE: u64 = Hashtable::SIZE * 8;

	/// Opens the hashtable and allocates necessary space on disk.
	pub fn open<P: AsRef<Path>>(path: P) -> Result<Hashtable> {
		let file = OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.open(path)?;

		file.set_len(Hashtable::FILE_SIZE)?;

		let hashtable = Hashtable {
			mmap: Mmap::open(&file, Protection::ReadWrite)?,
		};

		Ok(hashtable)
	}

	/// Splits hashtable into multiple independent parts.
	pub fn into_slices(self) -> Result<[HashtableSlice; DATABASE_SLICES]> {
		let part_size = Hashtable::FILE_SIZE as usize / DATABASE_SLICES;

		let mut mmap_view = self.mmap.into_view_sync();
		let mut result: [HashtableSlice; 16] = unsafe { mem::uninitialized() };

		for i in 0..DATABASE_SLICES - 1 {
			let (mut left, new_mmap_view) = mmap_view.split_at(part_size)?;
			left.restrict(0, part_size)?;
			let mut left_slice = left.into();
			mem::swap(&mut result[i], &mut left_slice);
			mem::forget(left_slice);
			mmap_view = new_mmap_view;
		}

		let remaining_len = mmap_view.len();
		mmap_view.restrict(0, remaining_len)?;
		let mut last_slice = mmap_view.into();
		mem::swap(&mut result[DATABASE_SLICES - 1], &mut last_slice);
		mem::forget(last_slice);
		Ok(result)
	}
}

impl From<MmapViewSync> for HashtableSlice {
	fn from(view: MmapViewSync) -> Self {
		HashtableSlice {
			view,
		}
	}
}

pub enum Entry<'a> {
	Occupied(OccupiedEntry),
	Vacant(VacantEntry<'a>),
}

pub struct OccupiedEntry {
	value: u64,
}

impl OccupiedEntry {
	pub fn get(&self) -> u64 {
		self.value
	}
}

pub struct VacantEntry<'a> {
	view: &'a mut [u8],
}

impl<'a> VacantEntry<'a> {
	pub fn insert(self, value: u64) {
		LittleEndian::write_u64(self.view, value);
	}
}

impl HashtableSlice {
	/// If slice query is known, returns the location of the key in the database,
	/// otherwise returns `None`.
	/// 
	/// ## Unsafety
	/// 
	/// The caller must ensure that the file is not concurrently modified.
	pub unsafe fn get(&self, query: SliceQuery) -> Option<u64> {
		let value = LittleEndian::read_u64(&self.view.as_slice()[query.0..query.0 + 8]);
		if value == 0 {
			None
		} else {
			Some(value)
		}
	}

	/// ## Unsafety
	/// 
	/// The caller must ensure that the file is not concurrently modified.
	pub unsafe fn entry(&mut self, query: SliceQuery) -> Entry {
		let view = &mut self.view.as_mut_slice()[query.0..query.0 + 8];
		let value = LittleEndian::read_u64(view);
		if value == 0 {
			Entry::Vacant(VacantEntry { view })
		} else {
			Entry::Occupied(OccupiedEntry { value })
		}
	}
}
