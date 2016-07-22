package org.blackcoinj.store;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bitcoinj.core.Address;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.StoredBlock;
import org.bitcoinj.core.StoredUndoableBlock;
import org.bitcoinj.core.UTXO;
import org.bitcoinj.core.UTXOProviderException;
import org.bitcoinj.core.Utils;
import org.bitcoinj.store.BlockStoreException;
import org.bitcoinj.store.FullPrunedBlockStore;
import org.blackcoinj.pos.BlackcoinMagic;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class H2MVStoreFullPrunedBlockstore implements FullPrunedBlockStore {
	private static final Logger log = LoggerFactory.getLogger(H2MVStoreFullPrunedBlockstore.class);

	private MVMap<byte[], byte[]> wholeMap;
	private MVStore store;
	private StoredBlock chainHead;
	private NetworkParameters params;
	private StoredBlock verifiedChainHead;
	private Sha256Hash theLast;
	private final long started;

	private final String CHAINHEAD = "CHAINHEAD";
	private final String VERIFIED_CHAINHEAD = "VERIFIED_CHAINHEAD";

	public H2MVStoreFullPrunedBlockstore(NetworkParameters params, String dbName) throws BlockStoreException {
		this.params = params;
		store = new MVStore.Builder().autoCommitDisabled().compressHigh().fileName(dbName).open();
		store.setReuseSpace(true);
		store.setStoreVersion(0);
		wholeMap = store.openMap("ALL");
		
		initStore();
		started = Utils.currentTimeSeconds();
	}

	private void initStore() throws BlockStoreException {
		byte[] verifiedChainHeadbytes = wholeMap.get(VERIFIED_CHAINHEAD.getBytes());
		byte[] chainHeadbytes = wholeMap.get(CHAINHEAD.getBytes());

		if (verifiedChainHeadbytes != null) {
			Sha256Hash hash = Sha256Hash.wrap(verifiedChainHeadbytes);
			StoredBlock headBlock = get(hash);
			this.verifiedChainHead = headBlock;
		}
		
		if (chainHeadbytes != null) {
			Sha256Hash hash = Sha256Hash.wrap(chainHeadbytes);
			StoredBlock headBlock = get(hash);
			this.chainHead = headBlock;
		}
		
		if(chainHeadbytes == null && this.verifiedChainHead != null)
			this.chainHead = this.verifiedChainHead;
		
		log.info("verifiedChainHead " + this.verifiedChainHead);
		log.info("chainHead " + this.chainHead); 
	}

	@Override
	public void put(StoredBlock block) throws BlockStoreException {
		insertOrUpdate(block, null);
	}

	private void insertOrUpdate(StoredBlock block, StoredUndoableBlock undoableBlock) throws BlockStoreException {
		Sha256Hash hash = block.getHeader().getHash();
		byte[] byteBlack = wholeMap.get(hash.getBytes());
		BlackBlock blackBlock = new BlackBlock(block, false, null, null);
		if (byteBlack != null) {
			blackBlock = new BlackBlock(params, byteBlack);			
		}
		if(undoableBlock != null){
			blackBlock.wasUndoable = true;
			blackBlock.txOutChanges = undoableBlock.getTxOutChanges();
			blackBlock.transactions = undoableBlock.getTransactions();
		}else{
			blackBlock.wasUndoable = false;
		}
		wholeMap.put(hash.getBytes(), blackBlock.toByteArray());
	}

	@Override
	public StoredBlock get(Sha256Hash hash) throws BlockStoreException {
		byte[] bytesBlack = wholeMap.get(hash.getBytes());
		if (bytesBlack == null) {
			return null;
		}

		BlackBlock storedBlock = new BlackBlock(params, bytesBlack);
		return storedBlock.block;
	}

	@Override
	public StoredBlock getChainHead() throws BlockStoreException {
		return chainHead;
	}

	@Override
	public void setChainHead(StoredBlock chainHead) throws BlockStoreException {
		this.chainHead = chainHead;
		wholeMap.put(CHAINHEAD.getBytes(), chainHead.getHeader().getHash().getBytes());
	}

	@Override
	public void close() throws BlockStoreException {
		if(Utils.currentTimeSeconds() - started > BlackcoinMagic.blockTime)
			removeAll();
		if(Utils.currentTimeSeconds() - started > BlackcoinMagic.blockTime * BlackcoinMagic.minimumStoreDepth)
			store.compactMoveChunks();
		store.close();
	}

	@Override
	public NetworkParameters getParams() {
		return params;
	}

	@Override
	public List<UTXO> getOpenTransactionOutputs(List<Address> addresses) throws UTXOProviderException {
		List<UTXO> foundOutputs = new ArrayList<UTXO>();
		for (byte[] output : wholeMap.values()) {
			if(output[0] == 1){
				continue;
			}	
			for (Address address : addresses) {
				ByteArrayInputStream in = new ByteArrayInputStream(output);
				UTXO outUTXO;
				try {
					outUTXO = new UTXO(in);
				} catch (IOException e) {
					throw new UTXOProviderException(e);
				}
				if (outUTXO.getAddress().equals(address.toString())) {
					foundOutputs.add(outUTXO);
				}
			}
		}
		return foundOutputs;
	}

	@Override
	public int getChainHeadHeight() throws UTXOProviderException {
		try {
			return getVerifiedChainHead().getHeight();
		} catch (BlockStoreException e) {
			throw new UTXOProviderException(e);
		}
	}

	@Override
	public void put(StoredBlock storedBlock, StoredUndoableBlock undoableBlock) throws BlockStoreException {
		insertOrUpdate(storedBlock, undoableBlock);
	}

	@Override
	public StoredBlock getOnceUndoableStoredBlock(Sha256Hash hash) throws BlockStoreException {
		byte[] byteBlock = wholeMap.get(hash.getBytes());
		if (byteBlock == null) {
			return null;
		}
		BlackBlock storedBlock = new BlackBlock(params, byteBlock);
		return storedBlock.wasUndoable ? storedBlock.block : null;
	}

	@Override
	public StoredUndoableBlock getUndoBlock(Sha256Hash hash) throws BlockStoreException {
		byte[] bytes = wholeMap.get(hash.getBytes());
		BlackBlock recoveredObj = new BlackBlock(params, bytes);
		if (recoveredObj.transactions == null) {
			return new StoredUndoableBlock(hash, recoveredObj.txOutChanges);
		}
		return new StoredUndoableBlock(hash, recoveredObj.transactions);
	}

	@Override
	public UTXO getTransactionOutput(Sha256Hash hash, long index) throws BlockStoreException {
		String point = String.valueOf(hash.toString()) + ":" + String.valueOf(index);
		byte[] byteUTXO = wholeMap.get(point.getBytes());
		if (byteUTXO == null) {
			return null;
		}
		ByteArrayInputStream in = new ByteArrayInputStream(byteUTXO);
		try {
			return new UTXO(in);
		} catch (IOException e) {
			throw new BlockStoreException(e);
		}
	}

	@Override
	public void addUnspentTransactionOutput(UTXO out) throws BlockStoreException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try {
			out.serializeToStream(bos);
		} catch (IOException e) {
			throw new BlockStoreException(e);
		}
		String point = String.valueOf(out.getHash().toString()) + ":" + String.valueOf(out.getIndex());
		wholeMap.put(point.getBytes(), bos.toByteArray());
	}

	@Override
	public void removeUnspentTransactionOutput(UTXO out) throws BlockStoreException {
		String point = String.valueOf(out.getHash().toString()) + ":" + String.valueOf(out.getIndex());
		if (wholeMap.remove(point.getBytes()) == null)
			throw new BlockStoreException(
					"Tried to remove a UTXO from MemoryFullPrunedBlockStore that it didn't have!");

	}

	@Override
	public boolean hasUnspentOutputs(Sha256Hash hash, int numOutputs) throws BlockStoreException {
		for (int i = 0; i < numOutputs; i++)
			if (getTransactionOutput(hash, i) != null)
				return true;
		return false;
	}

	@Override
	public StoredBlock getVerifiedChainHead() throws BlockStoreException {
		return verifiedChainHead;
	}

	@Override
	public void setVerifiedChainHead(StoredBlock chainHead) throws BlockStoreException {
		this.verifiedChainHead = chainHead;
		if (this.chainHead.getHeight() < chainHead.getHeight())
			setChainHead(chainHead);
		wholeMap.put(VERIFIED_CHAINHEAD.getBytes(), chainHead.getHeader().getHash().getBytes());
		
		
	}

	private void removeAll() throws BlockStoreException {
		log.info("removing all");
		Set<byte[]> keysList = wholeMap.keySet();
		int theHeight = chainHead.getHeight() - BlackcoinMagic.minimumStoreDepth;
		for(byte[] key : keysList) {
			byte[]output = wholeMap.get(key);
			if(output[0] == 1){
				try {
					BlackBlock storedBlock = new BlackBlock(params, output);
					if(storedBlock.block.getHeight() < theHeight)
						wholeMap.remove(key);
				} catch (BlockStoreException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
								
			}else{
				continue;
			}
		}
	}

	@Override
	public void beginDatabaseBatchWrite() throws BlockStoreException {
		// TODO Auto-generated method stub

	}

	@Override
	public void commitDatabaseBatchWrite() throws BlockStoreException {
		// TODO Auto-generated method stub

	}

	@Override
	public void abortDatabaseBatchWrite() throws BlockStoreException {
		// TODO Auto-generated method stub

	}

}
