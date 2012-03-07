package hdfs.replicationsimulator;

import hdfs.replicationsimulator.UnderReplicatedBlocks.BlockIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Random;

/**
 * 
 * This node store the metadata, the blocks locality and handle the replication
 * system
 * 
 * @author Corentone
 * @version 0.1
 */

public class Namenode extends Node {
	// Map every block
	private DatanodesMap DatanodesMap;
	private BlocksMap BlocksMap;

	private List<DatanodeInfo> heartbeats;
	private Queue<Event> toNamenode;
	private Queue<Event> toDatanodes;
	Object datanodeLock = new Object();

	public Daemon hbthread = null; // HeartbeatMonitor thread
	public Daemon replthread = null; // Replication thread
	public Daemon communicationthread = null; // Replication thread

	boolean isRunning = false;

	private UnderReplicatedBlocks neededReplications = new UnderReplicatedBlocks(Simulator.getNumberofReplicas());
	private PendingReplicationBlocks pendingReplications;

	// Duplicated in AllDatanode TODO
	private static final int BLOCK_SIZE = Simulator.getBlockSize()*1000;
	// heartbeatExpireInterval is how long namenode waits for datanode to report
	// heartbeat
	private long heartbeatExpireInterval = Simulator.getHeartbeat()*2+200;
	// replicationRecheckInterval is how often namenode checks for new
	// replication work
	private long replicationRecheckInterval = 2 * 1000L;
	// heartbeatRecheckInterval is how often namenode checks for expired datanodes
	private long heartbeatRecheckInterval = Math.round((Simulator.getHeartbeat()/5));
	//Communication recheck interval
	private long communicationRecheckInterval = 500L;
	
	
	private final int neededReplicas = Simulator.getNumberofReplicas();

	private int replIndex = 0;
	private long missingBlocksInCurIter = 0;
	private long missingBlocksInPrevIter = 0;
	volatile long pendingReplicationBlocksCount = 0L;
	volatile long corruptReplicaBlocksCount = 0L;
	volatile long underReplicatedBlocksCount = 0L;
	volatile long scheduledReplicationBlocksCount = 0L;
	volatile long excessBlocksCount = 0L;
	volatile long pendingDeletionBlocksCount = 0L;

	Random r = new Random();

	/*
	 * Constructor, Initialize threads and Maps
	 */
	public Namenode() {
		DatanodesMap = new DatanodesMap();
		BlocksMap = new BlocksMap();
		heartbeats = DatanodesMap.getHeartbeats();

		toNamenode = Simulator.getToNamenode();
		toDatanodes = Simulator.getToDatanodes();

		this.hbthread = new Daemon(new HeartbeatMonitor());
		this.replthread = new Daemon(new ReplicationMonitor());
		this.communicationthread = new Daemon(new CommunicationMonitor());

	}

	/*
	 * Add a node to the namenode (mainly for Init)
	 */
	void addNode(DatanodeInfo DatanodeInfo) {
		DatanodesMap.addDatanodeInfo(DatanodeInfo);
	}

	/*
	 * Add a block to the namenode (mainly for Init)
	 */
	void addBlock(BlockInfo Block) {
		BlocksMap.addBlock(Block);
	}

	/*
	 * Start the namenode (only contains replication system)
	 */
	void start() {
		isRunning = true;
		communicationthread.start();
		pendingReplications = new PendingReplicationBlocks();
		hbthread.start();
		replthread.start();
	}

	/**
	 * Periodically calls heartbeatCheck().
	 */
	class HeartbeatMonitor implements Runnable {
		/**
	   */
		public void run() {
			Simulator.addTrace(new SimTrace("Namenode heartbeat monitor created"));

			while (isRunning) {
				try {
					// TODO
					heartbeatCheck();
					Thread.sleep(heartbeatRecheckInterval);
				} catch (Exception e) {
					// FSNamesystem.LOG.error(StringUtils.stringifyException(e));
				}
			}
		}
	}

	/**
	 * Check if there are any expired heartbeats, and if so, whether any blocks
	 * have to be re-replicated. While removing dead datanodes, make sure that
	 * only one datanode is marked dead at a time within the synchronized
	 * section. Otherwise, a cascading effect causes more datanodes to be
	 * declared dead.
	 */
	void heartbeatCheck() {
		boolean allAlive = false;
		while (!allAlive) {
			boolean foundDead = false;
			int nodeID = -1;

			// locate the first dead node.
			synchronized (heartbeats) {
				for (Iterator<DatanodeInfo> it = heartbeats.iterator(); it
						.hasNext();) {
					DatanodeInfo nodeInfo = it.next();
					if (isDatanodeDead(nodeInfo)) {
						foundDead = true;
						nodeID = nodeInfo.getId();
						break;
					}
				}
			}

			// acquire the fsnamesystem lock, and then remove the dead node.
			if (foundDead) {
				synchronized (this) {

					synchronized (DatanodesMap) {
						DatanodeInfo nodeInfo = null;
						nodeInfo = DatanodesMap.getDatanodeInfo(nodeID);

						if (nodeInfo != null && isDatanodeDead(nodeInfo)) {
							/*
							 * NameNode.stateChangeLog.info(
							 * "BLOCK* NameSystem.heartbeatCheck: " +
							 * "lost heartbeat from " + nodeInfo.getName());
							 */
							Simulator.addTrace(new SimTrace(
									SimTrace.FAILURE_DETECTION, nodeInfo
											.getId()));
							removeDatanode(nodeInfo);

						}
					}
				}

			}
			allAlive = !foundDead;
		}
	}

	private void removeDatanode(DatanodeInfo nodeInfo) {
		DatanodesMap.removeDatanode(nodeInfo);

		/*
		for (Iterator<BlockInfo> it = nodeInfo.getBlockIterator(); it.hasNext();) {
			removeStoredBlock(it.next(), nodeInfo);
		}
		*/
		for(BlockInfo block: nodeInfo.getBlockList()){
			removeStoredBlock(block,nodeInfo);
		}

	}

	private boolean isDatanodeDead(DatanodeInfo node) {
		return (node.getLastHB() < (Node.now() - heartbeatExpireInterval));
	}

	/**
	 * Periodically calls computeReplicationWork().
	 */
	class ReplicationMonitor implements Runnable {
		static final int INVALIDATE_WORK_PCT_PER_ITERATION = 32;
		static final float REPLICATION_WORK_MULTIPLIER_PER_ITERATION = 2;

		public void run() {
			while (isRunning) {

					computeDatanodeWork();
					processPendingReplications();
					try {
						Thread.sleep(replicationRecheckInterval);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
			}
		}

		private void processPendingReplications() {
			BlockInfo[] timedOutItems = pendingReplications.getTimedOutBlocks();
			if (timedOutItems != null) {
				synchronized (this) {
					for (int i = 0; i < timedOutItems.length; i++) {
						int num = timedOutItems[i].numberOfReplicas();
						neededReplications.add(timedOutItems[i], num, 0,
								neededReplicas);
					}
				}
				/*
				 * If we know the target datanodes where the replication
				 * timedout, we could invoke decBlocksScheduled() on it. Its ok
				 * for now.
				 */
			}

		}
	}

	public int computeDatanodeWork() {
		int workFound = 0;
		int blocksToProcess = 0;
		int nodesToProcess = 0;
		// blocks should not be replicated or removed if safe mode is on

		synchronized (heartbeats) {
			blocksToProcess = (int) (heartbeats.size() * ReplicationMonitor.REPLICATION_WORK_MULTIPLIER_PER_ITERATION);
			nodesToProcess = (int) Math.ceil((double) heartbeats.size()
					* ReplicationMonitor.INVALIDATE_WORK_PCT_PER_ITERATION
					/ 100);
		}

		workFound = computeReplicationWork(blocksToProcess);

		// Update FSNamesystemMetrics counters
		synchronized (this) {
			pendingReplicationBlocksCount = pendingReplications.size();
			underReplicatedBlocksCount = neededReplications.size();
			scheduledReplicationBlocksCount = workFound;
		}

		return workFound;
	}

	/**
	 * Scan blocks in {@link #neededReplications} and assign replication work to
	 * data-nodes they belong to.
	 * 
	 * The number of process blocks equals either twice the number of live
	 * data-nodes or the number of under-replicated blocks whichever is less.
	 * 
	 * @return number of blocks scheduled for replication during this iteration.
	 */
	private int computeReplicationWork(int blocksToProcess) {
		// Choose the blocks to be replicated
		List<List<BlockInfo>> blocksToReplicate = chooseUnderReplicatedBlocks(blocksToProcess);

		// replicate blocks
		int scheduledReplicationCount = 0;
		for (int i = 0; i < blocksToReplicate.size(); i++) {
			for (BlockInfo block : blocksToReplicate.get(i)) {
				if (computeReplicationWorkForBlock(block, i)) {
					scheduledReplicationCount++;
				}
			}
		}
		return scheduledReplicationCount;
	}

	/**
	 * Get a list of block lists to be replicated The index of block lists
	 * represents the
	 * 
	 * @param blocksToProcess
	 * @return Return a list of block lists to be replicated. The block list
	 *         index represents its replication priority.
	 */
	synchronized List<List<BlockInfo>> chooseUnderReplicatedBlocks(
			int blocksToProcess) {
		// initialize data structure for the return value
		List<List<BlockInfo>> blocksToReplicate = new ArrayList<List<BlockInfo>>(
				UnderReplicatedBlocks.LEVEL);
		for (int i = 0; i < UnderReplicatedBlocks.LEVEL; i++) {
			blocksToReplicate.add(new ArrayList<BlockInfo>());
		}

		synchronized (neededReplications) {
			if (neededReplications.size() == 0) {
				missingBlocksInCurIter = 0;
				missingBlocksInPrevIter = 0;
				return blocksToReplicate;
			}

			// Go through all blocks that need replications.
			Iterator<BlockInfo> neededReplicationsIterator = neededReplications
					.iterator();
			// skip to the first unprocessed block, which is at replIndex
			for (int i = 0; i < replIndex
					&& neededReplicationsIterator.hasNext(); i++) {
				neededReplicationsIterator.next();
			}
			// # of blocks to process equals either twice the number of live
			// data-nodes or the number of under-replicated blocks whichever is
			// less
			blocksToProcess = Math.min(blocksToProcess,
					neededReplications.size());

			for (int blkCnt = 0; blkCnt < blocksToProcess; blkCnt++, replIndex++) {
				if (!neededReplicationsIterator.hasNext()) {
					// start from the beginning
					replIndex = 0;
					missingBlocksInPrevIter = missingBlocksInCurIter;
					missingBlocksInCurIter = 0;
					blocksToProcess = Math.min(blocksToProcess,
							neededReplications.size());
					if (blkCnt >= blocksToProcess)
						break;
					neededReplicationsIterator = neededReplications.iterator();
					assert neededReplicationsIterator.hasNext() : "neededReplications should not be empty.";
				}

				BlockInfo block = neededReplicationsIterator.next();
				int priority = ((BlockIterator) neededReplicationsIterator)
						.getPriority();
				if (priority < 0 || priority >= blocksToReplicate.size()) {
					// LOG.warn("Unexpected replication priority: " + priority +
					// " " + block);
				} else {
					blocksToReplicate.get(priority).add(block);
				}
			} // end for
		} // end synchronized
		return blocksToReplicate;
	}

	/**
	 * Replicate a block
	 * 
	 * @param block
	 *            block to be replicated
	 * @param priority
	 *            a hint of its priority in the neededReplication queue
	 * @return if the block gets replicated or not
	 */
	boolean computeReplicationWorkForBlock(BlockInfo block, int priority) {
		int requiredReplication, numEffectiveReplicas;
		List<DatanodeInfo> containingNodes;
		DatanodeInfo srcNode;

		synchronized (this) {
			synchronized (neededReplications) {
				containingNodes = block.getContainingNodes();
				requiredReplication = neededReplicas - containingNodes.size();

				// get a source data-node
				containingNodes = new ArrayList<DatanodeInfo>();
				int numReplicas = 0;// Useless in our SIMULATOR
				srcNode = chooseSourceDatanode(block, containingNodes,
						numReplicas);
				if (numReplicas <= 0) {
					missingBlocksInCurIter++;
				}
				if (srcNode == null) // block can not be replicated from any
										// node
					return false;

				// do not schedule more if enough replicas is already pending
				numEffectiveReplicas = numReplicas
						+ pendingReplications.getNumReplicas(block);
				if (numEffectiveReplicas >= requiredReplication) {
					neededReplications.remove(block, priority); // remove from
																// neededReplications
					replIndex--;
					/*
					 * NameNode.stateChangeLog.info("BLOCK* " +
					 * "Removing block " + block +
					 * " from neededReplications as it has enough replicas.");
					 */
					return false;
				}
			}
		}

		// choose replication targets: NOT HODING THE GLOBAL LOCK
		DatanodeInfo targets[] = chooseTarget(requiredReplication
				- numEffectiveReplicas, srcNode, containingNodes, BLOCK_SIZE);
		if (targets[0] == null)
			return false;

		synchronized (this) {
			synchronized (neededReplications) {
				// do not schedule more if enough replicas is already pending
				int numReplicas = BlocksMap.getcontainingNodes(block);
				numEffectiveReplicas = numReplicas
						+ pendingReplications.getNumReplicas(block);
				if (numEffectiveReplicas >= neededReplicas) {
					neededReplications.remove(block, priority); // remove from
																// neededReplications
					replIndex--;

					/*
					 * NameNode.stateChangeLog.info("BLOCK* " +
					 * "Removing block " + block +
					 * " from neededReplications as it has enough replicas.");
					 */
					return false;
				}

				// Add block to the to be replicated list
				// @TODO
				srcNode.addBlockToBeReplicated(block, targets);

				// Move the block-replication into a "pending" state.
				// The reason we use 'pending' is so we can retry
				// replications that fail after an appropriate amount of time.
				pendingReplications.add(block, targets.length);
				/*
				 * NameNode.stateChangeLog.debug( "BLOCK* block " + block +
				 * " is moved from neededReplications to pendingReplications");
				 */

				// remove from neededReplications
				if (numEffectiveReplicas + targets.length >= neededReplicas) {
					neededReplications.remove(block, priority); // remove from
																// neededReplications
					replIndex--;
				}
			}
		}

		return true;
	}

	private DatanodeInfo[] chooseTarget(int numReplicas, DatanodeInfo srcNode,
			List<DatanodeInfo> containingNodes, int blockSize) {

		DatanodeInfo[] nodes = new DatanodeInfo[numReplicas];
		DatanodeInfo chosen;

		nodes[0] = null;

		numReplicas = (numReplicas > DatanodesMap.getHeartbeats().size()) ? DatanodesMap
				.getHeartbeats().size() : numReplicas;
		int i = 0;
		do {
			chosen = DatanodesMap.randomNode();
			if (chosen.isGoodTarget(blockSize, containingNodes)) {
				numReplicas--;
				nodes[i] = chosen;
				i++;
			}
		} while (numReplicas > 0 || chosen == null);

		return nodes;
	}

	/**
	 * Parse the data-nodes the block belongs to and choose one, which will be
	 * the replication source.
	 * 
	 * We prefer nodes that are in DECOMMISSION_INPROGRESS state to other nodes
	 * since the former do not have write traffic and hence are less busy. We do
	 * not use already decommissioned nodes as a source. Otherwise we choose a
	 * random node among those that did not reach their replication limit.
	 * 
	 * In addition form a list of all nodes containing the block and calculate
	 * its replication numbers.
	 * 
	 * DESCRIPTION IS NOT REVELEVANT IN SIMULATOR
	 */
	private DatanodeInfo chooseSourceDatanode(BlockInfo block,
			List<DatanodeInfo> containingNodes, int numReplicas) {
		containingNodes.clear();
		DatanodeInfo srcNode = null;

		Iterator<DatanodeInfo> it = BlocksMap.nodeIterator(block);
		while (it.hasNext()) {
			DatanodeInfo node = it.next();

			// switch to a different node randomly
			// this to prevent from deterministically selecting the same node
			// even
			// if the node failed to replicate the block on previous iterations
			if (r.nextBoolean())
				srcNode = node;
			
			if (srcNode != null){
				return srcNode;
			}
		}
		return srcNode;
	}

	/**
	 * Periodically calls heartbeatCheck().
	 */
	class CommunicationMonitor implements Runnable {
		/**
	   */
		public void run() {
			while (isRunning) {
				try {
					// TODO
					CommunicationCheck();
					Thread.sleep(communicationRecheckInterval);
				} catch (Exception e) {
					// FSNamesystem.LOG.error(StringUtils.stringifyException(e));
				}
			}
		}
	}

	public void CommunicationCheck() {
		Event event;
		do {
			synchronized (toNamenode) {// Maybe the lock must be done on an
										// object
				event = toNamenode.poll();
			}

			if (event != null) {

				handleEvent(event);

			}

		} while (event != null);
	}

	private void handleEvent(Event event) {

		switch (event.getAction()) {
		case Event.HEARTBEAT:
			handleHeartbeat(event.getSource(), event.getTime());
			break;
		case Event.BLOCKRECEPTION:
			handleBlockReception(event.getSource(), event.getBlockId());
			break;
		}

	}

	private void handleBlockReception(int source, int blockId) {

		BlockInfo blockinfo = BlocksMap.getBlockInfo(blockId);
		DatanodeInfo datanode = DatanodesMap.getDatanodeInfo(source);

		// Add the block to the Datanode list
		datanode.addBlock(blockinfo);

		// Add the datanode to the block list
		blockinfo.addDatanode(datanode);

		// delete the pendingReplication entry
		pendingReplications.remove(blockinfo);

		Simulator.addTrace(new SimTrace(SimTrace.BLOCK_RECEIVED, source,
				blockId));
	}

	private void handleHeartbeat(int id, long time) {
		DatanodeInfo datanode = DatanodesMap.getDatanodeInfo(id);
		datanode.setLastHB(time);

		// Give the commands to the datanodes
		synchronized (Simulator.getToDatanodes()) {//TODO CHECK THAT
			for(Event command: datanode.getCommands()){
				this.addToDatanodeQueue(command);
			}
		}
	}

	public void initAddBlock(int DatanodeId, BlockInfo block) {
		DatanodeInfo datanode = DatanodesMap.getDatanodeInfo(DatanodeId);
		datanode.addBlock(block);
		block.addDatanode(datanode);

	}

	/**
	 * Modify (block-->datanode) map. Possibly generate replication tasks, if
	 * the removed block is still valid.
	 */
	synchronized void removeStoredBlock(BlockInfo block, DatanodeInfo node) {
		/*
		 * NameNode.stateChangeLog.debug("BLOCK* NameSystem.removeStoredBlock: "
		 * +block + " from "+node.getName());
		 */
		if (!BlocksMap.removeNode(block, node)) {
			/*
			 * NameNode.stateChangeLog.debug("BLOCK* NameSystem.removeStoredBlock: "
			 * 
			 * +block+" has already been removed from node "+node);
			 */
			return;
		}
		//
		// It's possible that the block was removed because of a datanode
		// failure. If the block is still valid, check if replication is
		// necessary. In that case, put block on a possibly-will-
		// be-replicated list.
		//
		// decrementSafeBlockCount(block);//TODO
		updateNeededReplications(block, -1, 0);// TODO
	}

	/* updates a block in under replication queue */
	synchronized void updateNeededReplications(BlockInfo block,
			int curReplicasDelta, int expectedReplicasDelta) {
		int repl = block.numberOfReplicas();
		int curExpectedReplicas = neededReplicas;

		Simulator.addTrace(new SimTrace(SimTrace.SCHEDULED_FOR_REPAIR, block
				.getId()));

		neededReplications.update(block, repl, 0, curExpectedReplicas,
				curReplicasDelta, expectedReplicasDelta);
	}

	public boolean addToDatanodeQueue(Event e) {

		synchronized (datanodeLock) {
			boolean result = toDatanodes.add(e);
			// System.out.println("added heartbeat to namenode.");
			return result;
		}
	}

	public BlocksMap getBlocksMap() {
		return BlocksMap;
	}
}
