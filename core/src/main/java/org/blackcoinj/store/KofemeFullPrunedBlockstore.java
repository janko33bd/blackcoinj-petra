package org.blackcoinj.store;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bitcoinj.core.Address;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.StoredBlock;
import org.bitcoinj.core.StoredUndoableBlock;
import org.bitcoinj.core.UTXO;
import org.bitcoinj.core.UTXOProviderException;
import org.bitcoinj.store.BlockStoreException;
import org.bitcoinj.store.FullPrunedBlockStore;
import org.blackcoinj.pos.BlackcoinMagic;
import org.fusesource.leveldbjni.JniDBFactory;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.MapSerializer;

public class KofemeFullPrunedBlockstore  implements FullPrunedBlockStore {
	private static final Logger log = LoggerFactory.getLogger(H2MVStoreFullPrunedBlockstore.class);

	private Map<ByteArrayWrapper, byte[]> wholeMap;
	private StoredBlock chainHead;
	private NetworkParameters params;
	private StoredBlock verifiedChainHead;
	private Sha256Hash theLast;

	private final String CHAINHEAD = "CHAINHEAD";
	private final String VERIFIED_CHAINHEAD = "VERIFIED_CHAINHEAD";
	private final String THE_LAST = "THE_LAST";

	private Kryo kryo;
	private final String dbname;

	private MapSerializer serializer;

	

	public KofemeFullPrunedBlockstore(NetworkParameters params, String dbName) throws BlockStoreException {
		this.params = params;
		this.dbname = dbName;
		kryo = new Kryo();
		serializer = new MapSerializer();
		kryo.register(HashMap.class, serializer);
		serializer.setKeyClass(ByteArrayWrapper.class, kryo.getSerializer(ByteArrayWrapper.class));
		serializer.setKeysCanBeNull(false);
		serializer.setValueClass(byte[].class, kryo.getSerializer(byte[].class));
		initMap(kryo, dbName);
		initStore();
	}

	private void initMap(Kryo kryo, String dbName) throws BlockStoreException {
		log.info("looking for " + dbName);
		Input input = null;
		try {
			input = new Input(new FileInputStream(new File(dbName)));
			wholeMap = kryo.readObject(input, HashMap.class, serializer);
		} catch (FileNotFoundException e) {
			throw new BlockStoreException("Couldn't find the file");
		}finally {
			if (input != null) {
				input.close();
		    }
		}
		
		if(wholeMap == null)
			throw new BlockStoreException("Couldn't read the file");
		
		createh2MvStore(wholeMap);
	}

	private void createh2MvStore(Map<ByteArrayWrapper, byte[]> wholeMap2) {
		File leveldbstore = new File("C:/MY/blackcoinj/latest/projects/multibithd/leveldb");
		
		if(!leveldbstore.exists()){
			Options options = new Options();
			options.createIfMissing(true);
			
			DB store = null;
			try {
				store = JniDBFactory.factory.open(leveldbstore, options);
			} catch (IOException e) {
				
			}
			Set<ByteArrayWrapper> keySet = wholeMap.keySet();
			for(ByteArrayWrapper key:keySet){
				store.put(key.data, wholeMap.get(key));
			}
			try {
				store.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	private void initStore() throws BlockStoreException {
		byte[] verifiedChainHeadbytes = wholeMap.get(new ByteArrayWrapper(VERIFIED_CHAINHEAD.getBytes()));
		byte[] chainHeadbytes = wholeMap.get(new ByteArrayWrapper(CHAINHEAD.getBytes()));
		byte[] lastBytes = wholeMap.get(new ByteArrayWrapper(THE_LAST.getBytes()));
		if (lastBytes != null) {
			this.theLast = Sha256Hash.wrap(lastBytes);
			log.info("setting to " + this.theLast.toString());
		} else
			log.info("not set");

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
		byte[] byteBlack = wholeMap.get(new ByteArrayWrapper(hash.getBytes()));
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
		wholeMap.put(new ByteArrayWrapper(hash.getBytes()), blackBlock.toByteArray());
	}

	@Override
	public StoredBlock get(Sha256Hash hash) throws BlockStoreException {
		byte[] bytesBlack = wholeMap.get(new ByteArrayWrapper(hash.getBytes()));
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
		wholeMap.put(new ByteArrayWrapper(CHAINHEAD.getBytes()), chainHead.getHeader().getHash().getBytes());
	}

	@Override
	public void close() throws BlockStoreException {
		serializeTofile(wholeMap);
	}

	private void serializeTofile(Map<ByteArrayWrapper, byte[]> wholeMap) throws BlockStoreException {
		Output output = null;
		try {
			output = new Output(new FileOutputStream(dbname));
			kryo.writeObject(output, wholeMap, serializer);
		} catch (FileNotFoundException e) {
			throw new BlockStoreException("Couldn't find the file");
		} finally {
			if (output != null) {
		          output.close();
		    }
		}
		System.out.println("saved");
	}

	@Override
	public NetworkParameters getParams() {
		return params;
	}

	@Override
	public List<UTXO> getOpenTransactionOutputs(List<Address> addresses) throws UTXOProviderException {
		List<UTXO> foundOutputs = new ArrayList<UTXO>();
		List<byte[]> outputsList = new ArrayList<byte[]>(wholeMap.values());
		for (byte[] output : outputsList) {
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
//		Sha256Hash hash = storedBlock.getHeader().getHash();
//		if (!hash.equals(Sha256Hash.wrap(BlackcoinMagic.checkpoint0)))
//			updatePrevWithNextBlock(storedBlock, undoableBlock);
		
		insertOrUpdate(storedBlock, undoableBlock);
		
	}

//	private void updatePrevWithNextBlock(StoredBlock storedBlock, StoredUndoableBlock undoableBlock)
//			throws BlockStoreException {
//		StoredBlock prevBlock = get(storedBlock.getHeader().getPrevBlockHash());
//		if (Sha256Hash.ZERO_HASH.equals(prevBlock.getHeader().getNextBlockHash())
//				|| prevBlock.getHeader().getNextBlockHash() == null) {
//			Sha256Hash prevBlockHash = prevBlock.getHeader().getHash();
//			byte[] byteBlock = wholeMap.get(new ByteArrayWrapper(prevBlockHash.getBytes()));
//			BlackBlock blackBlock = new BlackBlock(params, byteBlock);
//			blackBlock.block.getHeader().setNextBlockHash(storedBlock.getHeader().getHash());
//			wholeMap.put(new ByteArrayWrapper(prevBlockHash.getBytes()), blackBlock.toByteArray());
//		}
//
//	}

	@Override
	public StoredBlock getOnceUndoableStoredBlock(Sha256Hash hash) throws BlockStoreException {
		byte[] byteBlock = wholeMap.get(new ByteArrayWrapper(hash.getBytes()));
		if (byteBlock == null) {
			return null;
		}
		BlackBlock storedBlock = new BlackBlock(params, byteBlock);
		return storedBlock.wasUndoable ? storedBlock.block : null;
	}

	@Override
	public StoredUndoableBlock getUndoBlock(Sha256Hash hash) throws BlockStoreException {
		byte[] bytes = wholeMap.get(new ByteArrayWrapper(hash.getBytes()));
		BlackBlock recoveredObj = new BlackBlock(params, bytes);
		if (recoveredObj.transactions == null) {
			return new StoredUndoableBlock(hash, recoveredObj.txOutChanges);
		}
		return new StoredUndoableBlock(hash, recoveredObj.transactions);
	}

	@Override
	public UTXO getTransactionOutput(Sha256Hash hash, long index) throws BlockStoreException {
		String point = String.valueOf(hash.toString()) + ":" + String.valueOf(index);
		byte[] byteUTXO = wholeMap.get(new ByteArrayWrapper(point.getBytes()));
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
		wholeMap.put(new ByteArrayWrapper(point.getBytes()), bos.toByteArray());
	}

	@Override
	public void removeUnspentTransactionOutput(UTXO out) throws BlockStoreException {
		String point = String.valueOf(out.getHash().toString()) + ":" + String.valueOf(out.getIndex());
		if (wholeMap.remove(new ByteArrayWrapper(point.getBytes())) == null)
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
		setChainHead(chainHead);
		wholeMap.put(new ByteArrayWrapper(VERIFIED_CHAINHEAD.getBytes()), chainHead.getHeader().getHash().getBytes());
		if (this.theLast != null) {
			removeAll();
		}
	}


	private void removeAll() throws BlockStoreException {
		byte[] byteBlock = wholeMap.get(new ByteArrayWrapper(this.theLast.getBytes()));
		BlackBlock flagedBlock = new BlackBlock(params, byteBlock);
		wholeMap.remove(this.theLast.getBytes());
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
