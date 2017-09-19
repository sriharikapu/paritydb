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

	errors {
		InvalidKeyLen(expected: usize, got: usize) {
			description("Invalid key length")
			display("Invalid key length. Expected: {}, got: {}", expected, got),
		}
	}
}

impl PartialEq for ErrorKind {
	fn eq(&self, other: &Self) -> bool {
		use self::ErrorKind::*;

		match (self, other) {
			(&InvalidKeyLen(expected, got), &InvalidKeyLen(expected2, got2))
				if expected == expected2 && got == got2 => true,
			_ => false,
		}
	}
}
