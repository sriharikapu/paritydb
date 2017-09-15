//use std::ptr;
//use field::{Error};
//use find::{EmptySpace, find_empty_space};

// 3 => 4 // 3 * 4 / 3
// 6 => 8 // 6 * 4 / 3

//pub trait VirtualStore {
	//pub fn insert(&self, data: &[u8]);

	//pub fn flush(&self, data: &mut [u8]);
//}

//pub fn virtual_insert<V: VirtualStore>(data: &[u8], field_body_size: usize, record: &[u8], store: &V) -> Result<(), Error> {
	//unimplemented!();
//}

//pub fn insert(data: &mut [u8], field_body_size: usize, record: &[u8]) -> Result<(), Error> {
	//let mut required_space = record.len();

	//// reallocate existing records
	//let mut location = 0;
	//while required_space > 0 {
		//let space = find_empty_space(data, field_body_size, required_space)?;
		//match space {
			//EmptySpace::Found { offset, size }  => {
				//if size >= required_space {
					//location = offset;
					//break;
				//}

				//// to real size
				//let real_size = size * (field_body_size + 1) / field_body_size;

				//unsafe {
					//ptr::copy(data.as_ptr().offset(offset as isize), data.as_mut_ptr().offset(offset as isize), real_size);
				//}

				//required_space -= size;
			//},
			//EmptySpace::NotFound => {
				//unimplemented!();
			//}
		//}
	//};
	//unimplemented!();
//}
