package hdfs.replicationsimulator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

/*
 * Map to store information about blocks
 */
public class BlocksMap {

	private Map<Integer, BlockInfo> map;

	/*
	 * Constructor (Initialize the blocksMap)
	 */
	BlocksMap() {
		this.map = new HashMap<Integer, BlockInfo>();
	}

	/*
	 * Add a block to the map
	 */
	public void addBlock(BlockInfo block) {
		map.put(block.getId(), block);
	}

	/**
	 * Remove data-node reference from the block. Remove the block from the
	 * block map only if it does not belong to any file and data-nodes.
	 */
	boolean removeNode(BlockInfo b, DatanodeInfo node) {
		//BlockInfo info = map.get(b);
		BlockInfo info = b;
		if (info == null)
			return false;

		// remove block from the data-node block list (useless, but we never know) and the node from the block info 
		//node.removeBlock(info);//TODO Temporary comment, to check==> PROVOKE FAILURE
		boolean removed = info.removeDataNode(node);

		/*
		 * if (info.getDatanode(0) == null // no datanodes left && info.inode ==
		 * null) { // does not belong to a file map.remove(b); // remove block
		 * from the map }
		 */

		if (info.hasFailed()) {
			Simulator.addTrace( new SimTrace(SimTrace.DATA_LOSS,info.getId()));
		}
		return removed;
	}

	public Iterator<DatanodeInfo> nodeIterator(BlockInfo block) {
		return block.nodeIterator();
	}

	public int getcontainingNodes(BlockInfo block) {
		return block.numberOfReplicas();
	}

	public BlockInfo getBlockInfo(int blockId2) {
		return map.get(blockId2);
		
	}

}
