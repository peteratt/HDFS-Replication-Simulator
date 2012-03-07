package hdfs.replicationsimulator;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 
 * A datanode is storing data and execute commands to transfer files.
 * 
 * 
 * @author peteratt
 * @version 0.1
 */
public class Datanode extends Node {
	private int id;
	
	private long lastHB;
	
	private List<Block> blocks;

	/* 
	 * Status
	 */
	private long time_up;
	private long time_down;
	private boolean failed;
	
	private List<Event> commandQueue;
	
	private Queue<Block> pendingBlocks;

	public boolean blockChecking = false;
	
	public Datanode(int id, int capacity) {
		this.id=id;
		this.blocks = new ArrayList<Block>();
		this.lastHB = Node.now()-3000;
		this.commandQueue = new ArrayList<Event>();
		this.pendingBlocks = new ConcurrentLinkedQueue<Block>();
	}
	
	int getId(){
		return this.id;
	}
	
	
	/**
	 * Adds a command to the FIFO queue of the NameNode
	 * 
	 * @param e
	 * @return
	 */
	public boolean addCommand(Event e) {
		commandQueue.add(e);
		return true;
	}
	
	/*
	 * Add a block to the Local list of Nodes (Local)
	 */
	public void addBlock(Block block){
		blocks.add(block);
	}
	
	boolean hasFailed(){
		return failed;
	}

	public long getLastHB() {
		return lastHB;
	}

	public void setLastHB(long lastHB) {
		this.lastHB = lastHB;
	}

	public void setUploadingTime(long time) {
		this.time_up = time;
	}
	
	public long getUploadingTime() {
		return this.time_up;
	}
	
	public void setDownloadingTime(long time) {
		this.time_down = time;
	}
	
	public long getDownloadingTime() {
		return this.time_down;
	}

	/*
	public void setPendingBlock(int idBlock) {
		Block b = findBlockById(idBlock);
		pendingBlocks.add(b);
		if (blockChecking) {
			Thread t = new Thread(new BlockChecker());
			t.start();
		}
	}
	
	private Block findBlockById(int idBlock) {
		Block b = Simulator.getNamenode().getBlocksMap().getBlockInfo(idBlock);
		return b;
	}

	*/

	public boolean kill() {
		this.failed=true;
		return this.hasFailed();
	}
	
}
