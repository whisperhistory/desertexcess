

use std::collections::HashMap;

use rust_decimal::Decimal;

pub type TxId = u32;
pub type ClientId = u16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisputeState {
	Normal,
	Disputed,
	Resolved,
	ChargedBack,
}

impl Default for DisputeState {
	fn default() -> DisputeState {
		DisputeState::Normal
	}
}

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
	/// Tried to perform an action for which the client didn't have enough funds
	InsufficientFunds {
		available: Decimal,
		required: Decimal,
	},
	/// Got a reference to a tx we don't have
	TxNotFound {
		txid: TxId,
	},
	/// Got a tx referencing another tx that's in a state incompatible
	/// with the new transaction
	TxInWrongState {
		txid: TxId,
		action: TxType,
		state: DisputeState
	},
}

impl std::fmt::Display for Error {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		// might write a pretty formatter, using Debug for now
		std::fmt::Debug::fmt(self, f)
	}
}

impl std::error::Error for Error {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxType{
	Deposit,
	Withdrawal,
	Dispute,
	Resolve,
	Chargeback,
}

#[derive(Debug)]
struct Tx {
	/// The transaction ID
	txid: TxId,
	/// The transaction type
	tp: TxType,
	/// The client this transaction is from
	client: ClientId,
	/// The amount of the transaction
	amount: Decimal,
	/// In which state this tx is regarding disputes
	dispute_state: DisputeState,
}

#[derive(Debug, PartialEq, Eq)]
pub struct AccountSummary {
	/// The client this output represents
	pub client: ClientId,
	/// The total funds that are available for trading, staking, withdrawal, etc
	pub available: Decimal,
	/// The total funds that are held for dispute
	pub held: Decimal,
	/// The total funds that are available or held
	pub total: Decimal,
	/// Whether the account is locked
	pub locked: bool,
}

#[derive(Debug)]
struct Account {
	id: ClientId,
	/// The total funds that are available for trading, staking, withdrawal, etc
	available: Decimal,
	/// The total funds that are held for dispute
	held: Decimal,
	/// Whether the account is locked
	locked: bool,
}

impl Account {
	fn new(id: ClientId) -> Account {
		Account {
			id: id,
			available: Decimal::ZERO,
			held: Decimal::ZERO,
			locked: false,
		}
	}
}

impl Account {
	fn summary(&self) -> AccountSummary {
		AccountSummary {
			client: self.id,
			available: self.available,
			held: self.held,
			total: self.available + self.held,
			locked: self.locked,
		}
	}

	/// Utility function to assert that the account has sufficient available balance
	fn need(&self, required_amount: Decimal) -> Result<(), Error> {
		if self.available >= required_amount {
			Ok(())
		} else {
			Err(Error::InsufficientFunds {
				available: self.available,
				required: required_amount,
			})
		}
	}
}

pub struct Store {
	accounts: HashMap<ClientId, Account>,
	history: HashMap<TxId, Tx>,
}

impl Store {
	pub fn new() -> Store {
		Store {
			accounts: HashMap::new(),
			history: HashMap::new(),
		}
	}

	fn get_account(&mut self, id: ClientId) -> &mut Account {
		self.accounts.entry(id).or_insert_with(|| Account::new(id))
	}

	pub fn list_accounts(&self) -> impl Iterator<Item = AccountSummary> + '_ {
		self.accounts.values().map(|a| a.summary())
	}

	fn get_tx(&mut self, txid: TxId) -> Result<&mut Tx, Error> {
		self.history.get_mut(&txid).ok_or_else(|| Error::TxNotFound {
			txid: txid,
		})
	}

	pub fn handle_deposit(
		&mut self,
		txid: TxId,
		client: ClientId,
		amount: Decimal,
	) -> Result<(), Error> {
		assert!(amount.is_sign_positive());
		{
			let account = self.get_account(client);
			account.available += amount;
		}

		self.history.insert(txid, Tx {
			txid: txid,
			tp: TxType::Deposit,
			client: client,
			amount: amount,
			dispute_state: DisputeState::Normal,
		});
		Ok(())
	}

	pub fn handle_withdrawal(
		&mut self,
		txid: TxId,
		client: ClientId,
		amount: Decimal,
	) -> Result<(), Error> {
		assert!(amount.is_sign_positive());
		{
			let account = self.get_account(client);
			account.need(amount)?;
			account.available -= amount;
		}

		self.history.insert(txid, Tx {
			txid: txid,
			tp: TxType::Withdrawal,
			client: client,
			amount: amount,
			dispute_state: DisputeState::Normal,
		});
		Ok(())
	}

	pub fn handle_dispute(
		&mut self,
		client: ClientId,
		txid: TxId,
	) -> Result<(), Error> {
		let amount = {
			let tx = self.get_tx(txid)?;
			if tx.dispute_state != DisputeState::Normal {
				return Err(Error::TxInWrongState { txid, action: TxType::Dispute, state: tx.dispute_state });
			}

			// NB there's something strange in the assignment..
			// dispute only make sense on withdrawals, not really on deposits
			// but the math described in the assignment only makes sense for
			// deposits. this is an implementation according to the assignment,
			// but in practice a different dispute resolution should be applied
			// for different types of disputed transactions

			// since only withdrawals and deposits are logged in the history, assert this
			assert!(tx.tp == TxType::Withdrawal || tx.tp == TxType::Deposit,
				"impossible tx type disputed: {:?}", tx.tp,
			);

			tx.dispute_state = DisputeState::Disputed;
			tx.amount
		};
		
		let account = self.get_account(client);
		account.need(amount)?;
		account.available -= amount;
		account.held += amount;
		Ok(())
	}

	pub fn handle_resolve(
		&mut self,
		client: ClientId,
		txid: TxId,
	) -> Result<(), Error> {
		let amount = {
			let tx = self.get_tx(txid)?;
			if tx.dispute_state != DisputeState::Disputed {
				return Err(Error::TxInWrongState { txid, action: TxType::Resolve, state: tx.dispute_state });
			}
			tx.dispute_state = DisputeState::Resolved;

			// the tx type was already checked by the dispute tx
			tx.amount
		};
		
		let account = self.get_account(client);
		assert!(account.held >= amount);
		account.available += amount;
		account.held -= amount;
		Ok(())
	}

	pub fn handle_chargeback(
		&mut self,
		client: ClientId,
		txid: TxId,
	) -> Result<(), Error> {
		let amount = {
			let tx = self.get_tx(txid)?;
			if tx.dispute_state != DisputeState::Disputed {
				return Err(Error::TxInWrongState { txid, action: TxType::Chargeback, state: tx.dispute_state });
			}
			tx.dispute_state = DisputeState::ChargedBack;

			// the tx type was already checked by the dispute tx
			tx.amount
		};
		
		let account = self.get_account(client);
		assert!(account.held >= amount);
		account.held -= amount;
		account.locked = true;
		Ok(())
	}
}

#[cfg(test)]
mod test {
	use super::*;
	use rust_decimal::Decimal;

	/// Helper to create a decimal.
	fn d(s: &str) -> Decimal {
		s.parse().expect("invalid decimal")
	}

	#[test]
	fn simple_test() {
		let mut store = Store::new();
		let mut txid = 0; // an incrementing txid counter

		// The account ID we will use for our test user.
		const ACC: u16 = 100;

		// do a deposit
		txid += 1;
		store.handle_deposit(txid, ACC, d("5.12345")).unwrap();
		assert_eq!(store.get_account(ACC).summary(), AccountSummary {
			client: ACC,
			available: d("5.12345"),
			held: d("0"),
			total: d("5.12345"),
			locked: false,
		});

		// withdraw too much
		let ret = store.handle_withdrawal(txid, ACC, d("6")).unwrap_err();
		assert_eq!(ret, Error::InsufficientFunds { available: d("5.12345"), required: d("6") });

		// do a withdrawal
		txid += 1;
		store.handle_withdrawal(txid, ACC, d("4.01")).unwrap();
		assert_eq!(store.get_account(ACC).summary(), AccountSummary {
			client: ACC,
			available: d("1.11345"),
			held: d("0"),
			total: d("1.11345"),
			locked: false,
		});

		// do another deposit
		txid += 1;
		store.handle_deposit(txid, ACC, d("3")).unwrap();
		assert_eq!(store.get_account(ACC).summary(), AccountSummary {
			client: ACC,
			available: d("4.11345"),
			held: d("0"),
			total: d("4.11345"),
			locked: false,
		});
		let deposit_txid = txid;

		// dispute a non-existing tx
		assert_eq!(store.handle_dispute(ACC, 7).unwrap_err(), Error::TxNotFound { txid: 7 });

		// dispute it
		store.handle_dispute(ACC, deposit_txid).unwrap();
		assert_eq!(store.get_account(ACC).summary(), AccountSummary {
			client: ACC,
			available: d("1.11345"),
			held: d("3"),
			total: d("4.11345"),
			locked: false,
		});

		// dispute it again
		let ret = store.handle_dispute(ACC, deposit_txid).unwrap_err();
		assert_eq!(ret, Error::TxInWrongState {
			txid: deposit_txid,
			action: TxType::Dispute,
			state: DisputeState::Disputed,
		});

		// resolve it
		store.handle_resolve(ACC, deposit_txid).unwrap();
		assert_eq!(store.get_account(ACC).summary(), AccountSummary {
			client: ACC,
			available: d("4.11345"),
			held: d("0"),
			total: d("4.11345"),
			locked: false,
		});

		// dispute it again
		let ret = store.handle_dispute(ACC, deposit_txid).unwrap_err();
		assert_eq!(ret, Error::TxInWrongState {
			txid: deposit_txid,
			action: TxType::Dispute,
			state: DisputeState::Resolved,
		});
		// resolve it again
		let ret = store.handle_resolve(ACC, deposit_txid).unwrap_err();
		assert_eq!(ret, Error::TxInWrongState {
			txid: deposit_txid,
			action: TxType::Resolve,
			state: DisputeState::Resolved,
		});

		// chargeback it
		let ret = store.handle_chargeback(ACC, deposit_txid).unwrap_err();
		assert_eq!(ret, Error::TxInWrongState {
			txid: deposit_txid,
			action: TxType::Chargeback,
			state: DisputeState::Resolved,
		});

		// do another deposit
		txid += 1;
		store.handle_deposit(txid, ACC, d("9")).unwrap();
		assert_eq!(store.get_account(ACC).summary(), AccountSummary {
			client: ACC,
			available: d("13.11345"),
			held: d("0"),
			total: d("13.11345"),
			locked: false,
		});
		let deposit_txid = txid;

		// dispute it
		store.handle_dispute(ACC, deposit_txid).unwrap();
		assert_eq!(store.get_account(ACC).summary(), AccountSummary {
			client: ACC,
			available: d("4.11345"),
			held: d("9"),
			total: d("13.11345"),
			locked: false,
		});

		// charge it back
		store.handle_chargeback(ACC, deposit_txid).unwrap();
		assert_eq!(store.get_account(ACC).summary(), AccountSummary {
			client: ACC,
			available: d("4.11345"),
			held: d("0"),
			total: d("4.11345"),
			locked: true,
		});

		// charge it back again
		let ret = store.handle_chargeback(ACC, deposit_txid).unwrap_err();
		assert_eq!(ret, Error::TxInWrongState {
			txid: deposit_txid,
			action: TxType::Chargeback,
			state: DisputeState::ChargedBack,
		});
	}
}
