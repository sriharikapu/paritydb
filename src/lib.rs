//! Custom database for ethereum accounts
//! 
//! Assumptions:
//! 
//! - reads needs to be blazingly fast
//! 
//! - inserts not so much
//! 
//! - neither deletes
//! 
//! - guaranteed data integrity
//! 
//! - guaranteed data existance
//! 
//! Each record is stored in database as a header and body.
//! 
//! ```
//!  header    body
//!   /         / 
//! |...|...........|
//! ```
//! 
//! A header's length is either 1 or 9 bytes.
//! 
//! ```
//!  operation  body_legnth (optional)
//!   /          /
//! |...|...........|
//! ```
//! 
//! A body fields always consists of the key and the value
//! 
//! ```
//!  key      value 
//!   /         / 
//! |...|...........|
//! ```
//! 
//! The database consist of array of contant-size fields.
//! Record might be stored in one or more consecutive fields.
//! 
//! ```
//!  record_x            record_o
//!   /                    /
//! |xxxx|xxxx|xx..|....|oooo|o...|
//!  1234 1235 1236 1237 1238 1239
//! ```
//! 
//! Each field also has it's header and body.
//! 
//! ```
//!  header    body
//!   /         / 
//! |...|...........|
//! ```
//! 
//! A header is always a single byte which identicates what the body is.
//! 
//! ```
//! 0 - uninitialized
//! 1 - insert
//! 2 - continuation of the record
//! 3 - deleted record (the field can be reused)
//! ```
//! 
//! The index of the field for a record is determined using the first X bytes of the key.
//! If the field is already occupied we iterate over next fields until we find an empty one,
//! which has enough consecutive fields to store the record.

extern crate byteorder;
#[macro_use]
extern crate error_chain;
extern crate memmap;
extern crate parking_lot;
extern crate rayon;

mod database;
pub mod error;
pub mod file;
pub mod hashtable;

pub use database::Database;

/// Number of independent slices a database is dividable into.
/// Not configurable.
const DATABASE_SLICES: usize = 16;

type Address = [u8; 20];
type Hash = [u8; 32];
