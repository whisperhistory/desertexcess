
mod store;

use std::{env, fs, io};

use rust_decimal::Decimal;
use serde::{Serialize, Deserialize};

use store::{AccountSummary, Store};

#[derive(Debug, Deserialize)]
struct InputTx<'a> {
	#[serde(borrow)]
	tx_type: &'a str,
	client: u16,
	txid: u32,
	amount: Decimal,
}

#[derive(Debug, Serialize)]
struct OutputLine {
	client: u16,
	available: Decimal,
	held: Decimal,
	total: Decimal,
	locked: bool,
}

impl From<AccountSummary> for OutputLine {
	fn from(s: AccountSummary) -> OutputLine {
		OutputLine {
			client: s.client,
			available: s.available,
			held: s.held,
			total: s.total,
			locked: s.locked,
		}
	}
}


fn main() {
	let mut store = Store::new();

	let input_file = env::args().nth(1).expect("no input file provided");
	let input = fs::File::open(input_file).expect("failed to open input file");

	let mut reader = csv::ReaderBuilder::new()
		.buffer_capacity(1024^2)
		.delimiter(b',')
		.has_headers(true)
		.from_reader(io::BufReader::new(input));

	let stdout = io::stdout();
	let mut writer = csv::WriterBuilder::new()
		.buffer_capacity(1024^2)
		.delimiter(b',')
		.has_headers(true)
		.from_writer(stdout.lock());

	let mut record = csv::StringRecord::new();
	while reader.read_record(&mut record).expect("error reading CSV file") {
		let tx = record.deserialize::<InputTx>(None).expect("wrong format");

		let ret = match tx.tx_type {
			"deposit" => store.handle_deposit(tx.txid, tx.client, tx.amount),
			"withdraw" => store.handle_withdrawal(tx.txid, tx.client, tx.amount),
			"dispute" => store.handle_dispute(tx.client, tx.txid),
			"resolve" => store.handle_resolve(tx.client, tx.txid),
			"chargeback" => store.handle_chargeback(tx.client, tx.txid),
			_ => continue, // ignoring, should probably log error
		};

		if let Ok(account) = ret {
			let output: OutputLine = account.into();
			writer.serialize(output).expect("writing to stdout failed");
		} else {
			// handle error on ret, but spec says we should ignore errors, can't log either
			// perhaps log to stderr would be ok here
		}
	}
}
