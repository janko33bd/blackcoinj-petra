package org.blackcoinj.pos;

import static org.bitcoinj.script.ScriptOpCodes.OP_CHECKSIG;

import java.math.BigInteger;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spongycastle.crypto.params.KeyParameter;
import org.bitcoinj.core.AbstractBlockChain;
import org.bitcoinj.core.AbstractBlockChainListener;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.Coin;
import org.bitcoinj.core.Context;
import org.bitcoinj.core.ECKey;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.PeerGroup;
import org.bitcoinj.core.PrunedException;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.StoredBlock;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.TransactionConfidence;
import org.bitcoinj.core.TransactionInput;
import org.bitcoinj.core.TransactionOutPoint;
import org.bitcoinj.core.TransactionOutput;
import org.bitcoinj.core.UTXO;
import org.bitcoinj.core.Utils;
import org.bitcoinj.core.VerificationException;
import org.bitcoinj.core.Wallet;
import org.bitcoinj.script.Script;
import org.bitcoinj.script.ScriptBuilder;
import org.bitcoinj.script.ScriptChunk;
import org.bitcoinj.script.ScriptOpCodes;
import org.bitcoinj.store.BlockStoreException;
import org.bitcoinj.store.FullPrunedBlockStore;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import static com.google.common.base.Preconditions.checkArgument;

public class Staker extends AbstractExecutionThreadService {

	private static final Logger log = LoggerFactory.getLogger(Staker.class);

	private NetworkParameters params;
	private PeerGroup peers;
	private Wallet wallet;
	private FullPrunedBlockStore store;
	private AbstractBlockChain chain;
	private volatile boolean newBestBlockArrived = false;
	private volatile boolean stopStaking = false;
	private String walletPassword;
	private Map<Sha256Hash, RestoreUTXOut>stakedOuts;

	public Staker(NetworkParameters params, PeerGroup peers, Wallet wallet, FullPrunedBlockStore store,
			AbstractBlockChain chain, String walletPassword) {
		this.params = params;
		this.peers = peers;
		this.wallet = wallet;
		this.store = store;
		this.chain = chain;
		this.walletPassword = walletPassword;
    	List<TransactionOutput> calculateAllSpendCandidates = wallet.calculateAllSpendCandidates();
    	for(TransactionOutput txOut: calculateAllSpendCandidates){
    		log.info(String.valueOf(txOut.getValue().value));
    	}
    	stakedOuts = new HashMap<Sha256Hash, RestoreUTXOut>(); 
    	
	}

	private class MinerBlockChainListener extends AbstractBlockChainListener {

		@Override
		public void notifyNewBestBlock(StoredBlock storedBlock) throws VerificationException {
			log.info("notify new block");
			newBestBlockArrived = true;
		}

		@Override
		public void reorganize(StoredBlock splitPoint, List<StoredBlock> oldBlocks, List<StoredBlock> newBlocks)
				throws VerificationException {
			newBestBlockArrived = true;
		}

	}

	MinerBlockChainListener minerBlockChainListener = new MinerBlockChainListener();

	

	@Override
	protected void startUp() throws Exception {
		log.info("starting staking");
		super.startUp();
		chain.addListener(minerBlockChainListener);
	}

	@Override
	protected void shutDown() throws Exception {
		super.shutDown();
		log.info("shutting down staking");
		stopStaking = true;		
		chain.removeListener(minerBlockChainListener);
	}
	
	@Override	
	protected void triggerShutdown() {
		super.triggerShutdown();
		try {
			shutDown();
		} catch (Exception e) {
			throw new RuntimeException("Couldn't shutdown!");
		}
	}
	@Override
	protected void run() throws Exception {
		while (!stopStaking) {
			try {
				stake();

			} catch (Exception e) {
				log.error("Exception mining", e);
			}
		}
		shutDown();
	}

	void stake() throws Exception {
		newBestBlockArrived = false;
		
		StoredBlock prevBlock = chain.getChainHead();
		Transaction coinstakeTx = initCoinstakeTx();
		
		while (!stopStaking && !newBestBlockArrived
			&& isPastLasTime(prevBlock, coinstakeTx) && isFutureTime(coinstakeTx)){
					
			Thread.sleep(BlackcoinMagic.minerMiliSleep);
			prevBlock = chain.getChainHead();
			coinstakeTx = initCoinstakeTx();
						
		}

		while(!stopStaking && !newBestBlockArrived) {
			doStake(prevBlock, coinstakeTx);
			Thread.sleep(BlackcoinMagic.minerMiliSleep);
			coinstakeTx = initCoinstakeTx();
			if (isPastLasTime(prevBlock, coinstakeTx))
				break;
				
			if (isFutureTime(coinstakeTx))
				break;
							
		}
		
		if (newBestBlockArrived){
			log.info("block arrived");
		}
		

	}

	private boolean isFutureTime(Transaction coinstakeTx) {
		return coinstakeTx.getnTime() > Utils.currentTimeSeconds() + BlackcoinMagic.futureDrift;
	}

	private boolean isPastLasTime(StoredBlock prevBlock, Transaction coinstakeTx) {
		return coinstakeTx.getnTime() <= prevBlock.getHeader().getTimeSeconds() + BlackcoinMagic.futureDrift;
	}

	private Transaction initCoinstakeTx() {
		Transaction coinstakeTx = new Transaction(params);
		// apply black magic https://en.wikipedia.org/wiki/Bitwise_operation#Mathematical_equivalents
		coinstakeTx.setnTime(coinstakeTx.getnTime() & ~BlackcoinMagic.STAKE_TIMESTAMP_MASK);
		// Mark coin stake transaction
		coinstakeTx.addOutput(new TransactionOutput(params, null, Coin.ZERO, new byte[0]));
		return coinstakeTx;
	}

	private Transaction createCoinbaseTx(StoredBlock prevBlock) {
		Transaction coinbaseTransaction = new Transaction(params);
		// max length 100
		// Height first in coinbase required for block.version=2
		int blockHeight = prevBlock.getHeight() + 1;
		byte[] coinbaseScript = createCoinbaseScript(blockHeight);
		TransactionInput ti = new TransactionInput(params, coinbaseTransaction, coinbaseScript);
		coinbaseTransaction.addInput(ti);
		coinbaseTransaction.addOutput(new TransactionOutput(params, coinbaseTransaction, Coin.ZERO, new byte[0]));
		return coinbaseTransaction;
	}
	
//	 first byte is number of bytes in the number 
//	 (will be 0x03 on main net for the next 150 or so years with 223-1 blocks), 
//	 following bytes are little-endian representation of the number (including a sign bit).	
	private byte[] createCoinbaseScript(int blockHeight) {
		byte[] out = new byte[3];
 	   	out[0] = (byte) (0xFF & blockHeight);
 	   	out[1] = (byte) (0xFF & (blockHeight >> 8));
 	   	out[2] = (byte) (0xFF & (blockHeight >> 16));
 	   	
 	   	ScriptBuilder coinbaseScriptBld = new ScriptBuilder();
	   	ScriptChunk cnk = new ScriptChunk(ScriptOpCodes.OP_COINBASE, out);
	   	coinbaseScriptBld.addChunk(cnk);
 	   	Script coinbaseScript = coinbaseScriptBld.build();
 	   	log.info(coinbaseScript.toString());
 	   	return coinbaseScript.getProgram();
	}
	
	private void doStake(StoredBlock prevBlock, Transaction coinstakeTx) throws BlockStoreException {
		Sha256Hash stakeKernelHash;
		long stakeTxTime = coinstakeTx.getnTime();
		// TODO select coins for staking
		List<TransactionOutput> calculateAllSpendCandidates = wallet.calculateAllSpendCandidates();
		BigInteger bigNextTargetRequired = params.getNextTargetRequired(prevBlock, store);
		long difficultyTarget = Utils.encodeCompactBits(bigNextTargetRequired);
		for (TransactionOutput candidate : calculateAllSpendCandidates) {
			// if (CheckKernel(pindexPrev, nBits, txNew.nTime,
			// prevoutStake, &nBlockTime[not needed]))
			stakeKernelHash = checkForKernel(prevBlock, difficultyTarget, stakeTxTime, candidate);
			if (stakeKernelHash != null) {
				Set<Transaction> transactionsToInclude = getTransactionsToInclude(prevBlock.getHeight());
				Coin fees = getFees(transactionsToInclude);
				log.info("kernel found");
				Coin reward = candidate.getValue();
				reward = reward.add(Coin.valueOf(1, 50));
				reward = reward.add(fees);
				log.info("reward: " + reward);
				log.info("candidate: " + candidate.getValue());
				log.info("fees: " +fees.getValue());
				if(reward.isLessThan(candidate.getValue())){
					throw new BlockStoreException("coinstake destroys money!!");
				}
				
				ECKey key = findWholeKey(candidate);
				if(key == null)
					continue;
				
				Script keyScript = new ScriptBuilder().data(key.getPubKey()).op(OP_CHECKSIG).build();
				addCoinstakeOutput(coinstakeTx, reward, keyScript);
				checkCoinStake(coinstakeTx, reward);
				
				coinstakeTx.addSignedInput(candidate, key);
				try {
					coinstakeTx.verify();
					coinstakeTx.getInputs().get(0).verify();
				} catch (VerificationException ex) {
					throw new BlockStoreException(ex);
				}

				Transaction coinbaseTransaction = createCoinbaseTx(prevBlock);
				coinbaseTransaction.setnTime(coinstakeTx.getnTime());
				
				Block newBlock = new Block(params, BlackcoinMagic.blockVersion, prevBlock.getHeader().getHash(),
						coinstakeTx.getnTime(), difficultyTarget);
				newBlock.addTransaction(coinbaseTransaction);
				newBlock.addTransaction(coinstakeTx);
				for (Transaction transaction : transactionsToInclude) {
					newBlock.addTransaction(transaction);
				}
				
				byte[] blockSignature = key.signReversed(newBlock.getHash()).encodeToDER();

				newBlock.setSignature(blockSignature);
				
				log.info("broadcasting: " + newBlock.getHash());
				peers.broadcastMinedBlock(newBlock);
				
				TransactionOutPoint prevoutStake = candidate.getOutPointFor();
				UTXO stakedUTXO = store.getTransactionOutput(prevoutStake.getHash(), prevoutStake.getIndex());				
				RestoreUTXOut utxoOut = new RestoreUTXOut(stakedUTXO, candidate, coinstakeTx);
				
				stakedOuts.put(newBlock.getHash(), utxoOut);
				
				try {
					chain.add(newBlock);
				} catch (VerificationException verExc) {
					throw new RuntimeException(verExc);
				} catch (PrunedException prunExc) {
					throw new RuntimeException(prunExc);
				}
				
				log.info("Sent mined block: " + newBlock.getHash());
				log.info("blocktime " + newBlock.getTimeSeconds());
				log.info("coinstakeTx " + coinstakeTx.getnTime());
				break;
				
			}

		}
	}

	private void addCoinstakeOutput(Transaction coinstakeTx, Coin reward, Script keyScript) {
		if (reward.isGreaterThan(Coin.valueOf(BlackcoinMagic.spliStakeLimitCoins, 0))) {
			long split = reward.value / 2;
			coinstakeTx.addOutput(Coin.valueOf(split), keyScript);
			coinstakeTx.addOutput(Coin.valueOf(reward.value - split), keyScript);
			checkArgument(coinstakeTx.getOutputs().size() == 3);
		} else {
			coinstakeTx.addOutput(reward, keyScript);
			checkArgument(coinstakeTx.getOutputs().size() == 2);
		}

	}

	private Coin getFees(Set<Transaction> transactionsToInclude) {
		Coin fees = Coin.ZERO;
		for(Transaction tx:transactionsToInclude){
			fees = fees.add(tx.getFee());
		}
		return fees;
	}

	private void checkCoinStake(Transaction coinstakeTx, Coin reward) throws BlockStoreException {
		Coin value = Coin.ZERO;
		for(TransactionOutput out:coinstakeTx.getOutputs()){
			value = value.add(out.getValue());
		}
		if (value.isLessThan(reward)){
			throw new BlockStoreException("coinstake destroys money!");
		}
	}

	private ECKey findWholeKey(TransactionOutput candidate) throws BlockStoreException {
		ECKey wholeKey = null;
		try{
			byte[] patternBytes = candidate.getScriptPubKey().getPubKey();
			wholeKey = wallet.findKeyFromPubKey(patternBytes);
		}catch(Exception ex){
			byte[] patternBytes = candidate.getScriptPubKey().getPubKeyHash();
			wholeKey = wallet.findKeyFromPubHash(patternBytes);
		}
		
		if (wholeKey!=null) {
			if(wholeKey.getKeyCrypter() != null){
				log.info("decrypting");
	            KeyParameter aesKey = wholeKey.getKeyCrypter().deriveKey(walletPassword);
	            return wholeKey.decrypt(aesKey);
			}
			return wholeKey;
		}  
		log.info("no key found");
		return wholeKey;
	}

	private Sha256Hash checkForKernel(StoredBlock prevBlock, long difficultyTarget, long stakeTxTime,
			TransactionOutput candidate) throws BlockStoreException {
		Sha256Hash stakeKernelHash = null;

		
			TransactionOutPoint prevoutStake = candidate.getOutPointFor();
			UTXO txPrev = store.getTransactionOutput(prevoutStake.getHash(), prevoutStake.getIndex());
			if (txPrev == null) {
				log.info("can't check for kernel");
				return stakeKernelHash;
			}

			BlackcoinPOS blkPOS = new BlackcoinPOS(store);
			stakeKernelHash = blkPOS.checkStakeKernelHash(prevBlock, difficultyTarget, txPrev, stakeTxTime,
					prevoutStake);
		
		return stakeKernelHash;
	}

	private Set<Transaction> getTransactionsToInclude(int prevHeight) throws BlockStoreException {
		chain.getLock().lock();
		try {
			Context context = new Context(params);
			List<TransactionConfidence> list = context.getConfidenceTable().getAll();
			Set<TransactionOutPoint> spentOutPointsInThisBlock = new HashSet<TransactionOutPoint>();
			Set<Transaction> transactionsToInclude = new TreeSet<Transaction>(new TransactionPriorityComparator());
			for (TransactionConfidence txConf : list) {
				if (txConf != null) {
					Transaction tx = wallet.getTransaction(txConf.getTransactionHash());
					if (tx != null && !store.hasUnspentOutputs(tx.getHash(), tx.getOutputs().size())) {
						// Transaction was not already included in a block that
						// is
						// part of the best chain
						boolean allOutPointsAreInTheBestChain = true;
						boolean allOutPointsAreMature = true;
						boolean doesNotDoubleSpend = true;
						for (TransactionInput transactionInput : tx.getInputs()) {
							TransactionOutPoint outPoint = transactionInput.getOutpoint();
							UTXO storedOutPoint = store.getTransactionOutput(outPoint.getHash(), outPoint.getIndex());
							if (storedOutPoint == null) {
								// Outpoint not in the best chain
								allOutPointsAreInTheBestChain = false;
								break;
							}
							if ((prevHeight + 1) - storedOutPoint.getHeight() < params.getSpendableCoinbaseDepth()) {
								// Outpoint is a non mature coinbase
								allOutPointsAreMature = false;
								break;
							}
							if (spentOutPointsInThisBlock.contains(outPoint)) {
								doesNotDoubleSpend = false;
								break;
							} else {
								spentOutPointsInThisBlock.add(outPoint);
							}

						}
						if (allOutPointsAreInTheBestChain && allOutPointsAreMature && doesNotDoubleSpend) {
							transactionsToInclude.add(tx);
						}
					}
				}

			}
			if (!transactionsToInclude.isEmpty()) {
				long pastTime = chain.getChainHead().getHeader().getTimeSeconds();
				for (Transaction transaction : transactionsToInclude) {
					if (pastTime < transaction.getnTime())
						transactionsToInclude.remove(transaction);
				}
				return ImmutableSet.copyOf(Iterables.limit(transactionsToInclude, 1000));
			}

			return ImmutableSet.of();
		} finally {
			chain.getLock().unlock();
		}

	}

	private static class TransactionPriorityComparator implements Comparator<Transaction> {
		@Override
		public int compare(Transaction tx1, Transaction tx2) {
			int updateTimeComparison = tx1.getUpdateTime().compareTo(tx2.getUpdateTime());
			// If time1==time2, compare by tx hash to make comparator consistent
			// with equals
			return updateTimeComparison != 0 ? updateTimeComparison : tx1.getHash().compareTo(tx2.getHash());
		}
	}

}