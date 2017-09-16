#![allow(unknown_lints)]
#![allow(missing_docs)]

use std::{io, num};

error_chain! {
	foreign_links {
		Io(io::Error);
		Num(num::ParseIntError);
	}
}
