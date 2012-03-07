package hdfs.replicationsimulator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This is the DataInfo that the namenode stores
 */
public class DatanodeInfo {
	
	private int capacity;
	private int id;
	private long lastHB;
	private List<BlockInfo> blocks;
	
	private List<Event> commands;
	Object datanodeLock = new Object();
	
	public DatanodeInfo(int id, int capacity) {
		this.id = id;
		this.blocks = new ArrayList<BlockInfo>();
		this.capacity = capacity;
		this.lastHB = Node.now(); //To correct problems at startup
		this.commands = new ArrayList<Event>();
	}
	
	int getId() {
		return this.id;
	}

	public long getLastHB() {
		return lastHB;
	}

	public void setLastHB(long lastHB) {
		this.lastHB = lastHB;
	}
	
	public void addBlock(BlockInfo block){
		blocks.add(block);
	}
	
	public boolean removeBlock(Block block){
		return blocks.remove(block);
	}
	
	Iterator<BlockInfo> getBlockIterator() {
	    return this.blocks.iterator();
	  }
	
	List<BlockInfo> getBlockList(){
		return blocks;
	}

	public int getCapacity() {
		return this.capacity;
	}
	
	public boolean isGoodTarget(int blockSize, List<DatanodeInfo> containingNodes) {
		return (this.getCapacity()>=blockSize && !containingNodes.contains(this.id));
	}

	public void addBlockToBeReplicated(BlockInfo block, DatanodeInfo[] targets) {
		
		for(int i=0; i < targets.length; i++){
			commands.add(new Event(this.id, Event.REPLICATION, 0L, targets[i].getId(), block.getId()));
		}
		
		
	}

	public List<Event> getCommands() {
		return commands;
	}
}
