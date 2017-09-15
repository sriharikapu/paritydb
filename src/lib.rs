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
//! ```text
//!  header    body
//!   /         / 
//! |...|...........|
//! ```
//! 
//! A header's length is either 0 or 4 bytes.
//! 
//! ```text
//!  operation  body_legnth (optional)
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
extern crate memmap;
extern crate parking_lot;
extern crate rayon;
extern crate tiny_keccak;

pub mod error;
pub mod field;
pub mod find;
pub mod insert;
pub mod journal;
pub mod record;
pub mod transaction;
