extern crate clap;
extern crate paritydb;

use clap::{Arg, ArgMatches, App, SubCommand};
use paritydb::{Database, Error, Options, Transaction};

fn read_parameters<'a>(matches: &'a ArgMatches) -> Result<(&'a str, &'a str, Option<&'a str>), ()>{
	match (matches.value_of("DB"), matches.value_of("KEY")) {
		(Some(db), Some(key)) => {
			Ok((db, key, matches.value_of("VALUE")))
		},
		_ => {
			Err(())
		}
	}
}

fn do_insert(db: &str, key: &str, value: &str) -> Result<(), Error> {
	let mut db = Database::open(db, Options::default())
				.or(Database::create(db, Options::default()))?;
	let mut tx = Transaction::default();
	tx.insert(key, value);
	db.commit(&tx)?;
	db.flush_journal(1)?;
	Ok(())
}

fn do_delete(db: &str, key: &str) -> Result<(), Error> {
	let mut db = Database::open(db, Options::default())?;
	let mut tx = Transaction::default();
	tx.delete(key);
	db.commit(&tx)?;
	db.flush_journal(1)?;
	Ok(())
}

fn main() {
	let matches =
		App::new("paritydb-cli")
			.version("0.1.0")
			.author("Parity Technology")
			.about("A simple command line interface for ParityDB")
			.subcommand(SubCommand::with_name("insert")
				.about("Insert key to database")
				.arg(Arg::with_name("KEY")
					.short("k")
					.long("key")
					.takes_value(true))
				.arg(Arg::with_name("VALUE")
					.short("v")
					.long("value")
					.takes_value(true))
				.arg(Arg::with_name("DB")
					.short("d")
					.long("db")
					.takes_value(true)))
			.subcommand(SubCommand::with_name("delete")
				.about("Delete key in database")
				.arg(Arg::with_name("KEY")
					.short("k")
					.long("key")
					.takes_value(true))
				.arg(Arg::with_name("DB")
					.short("d")
					.long("db")
					.takes_value(true)))
			.get_matches();

	match matches.subcommand() {
		("insert", Some(sub_m)) => {
			if let Ok((db, key, Some(value))) = read_parameters(&sub_m) {
				do_insert(db, key, value).expect("execute insert error.");
			} else {
				println!("errors for insert.");
			}
		},
		("delete", Some(sub_m)) => {
			if let Ok((db, key, _)) = read_parameters(&sub_m) {
				do_delete(db, key).expect("execute delete error.");
			} else {
				println!("errors for delete");
			}
		},
		_ => {}
	}
}
