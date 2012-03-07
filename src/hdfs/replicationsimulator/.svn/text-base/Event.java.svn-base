package hdfs.replicationsimulator;

public class Event {
	
	private int source;
	private int action;
	private long time;
	private int destination;
	private int blockId;
	
	public final static int HEARTBEAT = 1;
	public final static int BLOCKRECEPTION = 2;
	
	public final static int REPLICATION = 3;
	public final static int PENDINGTRANSFER = 4;
	
	
	public static final int FAILURE = 0;
	
	
	public Event(int source, int action, long time) {
		this.source = source;
		this.action = action;
		this.time = time;
	}


	public Event(int source, int action, long time, int param1) {
		this.source = source;
		this.action = action;
		this.time = time;
		this.destination = param1;
	}


	public Event(int source, int action, long time, int destination, int blockId) {
		this.source = source;
		this.action = action;
		this.time = time;
		this.destination = destination;
		this.blockId = blockId;
	}


	public int getSource() {
		return source;
	}


	public int getAction() {
		return action;
	}


	public long getTime() {
		return time;
	}


	public int getDestination() {
		return destination;
	}


	public int getBlockId() {
		return blockId;
	}
	
}
