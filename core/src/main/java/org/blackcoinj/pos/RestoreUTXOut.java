package org.blackcoinj.pos;

import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.TransactionOutput;
import org.bitcoinj.core.UTXO;

public class RestoreUTXOut {
	
	private UTXO utxo;
	private TransactionOutput out;
	private Transaction coinstakeTx;
	
	public RestoreUTXOut(UTXO utxo, TransactionOutput out, Transaction coinstakeTx) {
		super();
		this.utxo = utxo;
		this.out = out;
		this.coinstakeTx = coinstakeTx;
	}

	public UTXO getUtxo() {
		return utxo;
	}

	public TransactionOutput getOut() {
		return out;
	}

	public Transaction getCoinstakeTx() {
		return coinstakeTx;
	}
	
	
	
}
