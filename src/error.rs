#![allow(unknown_lints)]
#![allow(missing_docs)]

use std::io;

error_chain! {
	foreign_links {
		Io(io::Error);
	}
}
