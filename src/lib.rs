//! Custom database for ethereum accounts
//!
//! Assumptions:
//!
//! - key-value database
//!
//! - with blazingly fast reads
//!
//! - not so fast inserts
//!
//! - neither deletes
//!
//! - guaranteed ACID (atomicity, consistency, isolation and durability)
//!
//! Each record is stored in database as a header and body.
//!
//! ```text
//!  header    body
//!   /         /
//! |...|...........|
//! ```
//!
//! A header's length is either 0 or 4 bytes.
//!
//! ```text
//!  operation  body_length (optional)
//!   /          /
//! |...|...........|
//! ```
//!
//! A body fields always consists of the key and the value
//!
//! ```text
//!  key      value
//!   /         /
//! |...|...........|
//! ```
//!
//! The database consist of array of contant-size fields.
//! Record might be stored in one or more consecutive fields.
//!
//! ```text
//!  record_x            record_o
//!   /                    /
//! |xxxx|xxxx|xx..|....|oooo|o...|
//!  1234 1235 1236 1237 1238 1239
//! ```
//!
//! Each field also has it's header and body.
//!
//! ```text
//!  header    body
//!   /         /
//! |...|...........|
//! ```
//!
//! A header is always a single byte which identicates what the body is.
//!
//! ```text
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
extern crate hex_slice;
extern crate memmap;
extern crate parking_lot;
extern crate rayon;
extern crate tiny_keccak;

mod database;
mod error;
mod field;
mod find;
mod flush;
mod journal;
mod key;
mod options;
mod record;
mod space;
mod transaction;

pub use database::{Database, Value};
pub use error::{Error, Result, ErrorKind};
pub use options::{Options, ValuesLen};
pub use record::Record;
pub use transaction::Transaction;
