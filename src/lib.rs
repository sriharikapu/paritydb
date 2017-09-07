//! Custom database for ethereum accounts
//! 
//! Assumptions:
//! - optimized for inserts
//! - optimized for parallel reads
//! - not-optimized for deletes
//! - guaranteed data integrity
//! - not guaranteed inserted data existence (fast io trade-off)

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
