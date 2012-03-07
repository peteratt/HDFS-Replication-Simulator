package hdfs.replicationsimulator;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class SimTrace {
	
	public static final int FAILURE_DETECTION = 0;
	public static final int SCHEDULED_FOR_REPAIR = 1;
	public static final int DATA_LOSS = 2;
	public static final int BLOCK_RECEIVED = 3;
	public static final int INFO = 4;
	
	private long timestamp;
	private String message;
	
	private int action;
	private int id;
	private int idBlock;
	
	public SimTrace(long timestamp, String msg) {
		this.timestamp = timestamp;
		this.message = msg;
	}
	
	public SimTrace(int action, int id) {
		this.timestamp = Node.now();
		this.action = action;
		this.id = id;
		
		switch (action) {
		
		case FAILURE_DETECTION:
			this.message = "Namenode has detected a failure in Datanode #" + id;
			break;
			
		case SCHEDULED_FOR_REPAIR:
			this.message = "Namenode has scheduled to repair block #" + id;
			break;
			
		case DATA_LOSS:
			this.message = "Block #" + id + " is lost";
			break;
		}
	}
	
	public SimTrace(String info) {
		this.timestamp = Node.now();
		this.action = INFO;
		this.id = 0;
		this.message = info;
	}
	
	public SimTrace(int action, int id, int idBlock) {
		this.timestamp = Node.now();
		this.action = action;
		this.id = id;
		this.idBlock = idBlock;
		
		switch (action) {
			
		case BLOCK_RECEIVED:
			this.message = "Datanode #" + id + " has received a block #" + idBlock;
			break;
		}
	}
	
	public long getTimestamp() {
		return timestamp;
	}
	
	public String getMessage() { 
		return message;
	}
	
	@Override
	public String toString() {
		
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(timestamp);
		
		DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS");
		
		return "[" + formatter.format(calendar.getTime()) + "] " + message + "\n";
	}

	public int getAction() {
		return action;
	}

	public int getId() {
		return id;
	}

	public boolean isLossEvent() {
		return (action == DATA_LOSS);
	}

	public int getIdBlock() {
		return idBlock;
	}

}
