#![allow(unknown_lints)]
#![allow(missing_docs)]

use std::{io, num};

use field;

error_chain! {
	links {
		Field(field::Error, field::ErrorKind);
	}

	foreign_links {
		Io(io::Error);
		Num(num::ParseIntError);
	}
}
