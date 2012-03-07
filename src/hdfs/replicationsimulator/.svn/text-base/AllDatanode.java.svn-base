package hdfs.replicationsimulator;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class AllDatanode {

	private List<Datanode> datanodes;
	private boolean isRunning = false;
	private Queue<Event> toNamenode;

	private Queue<Event> toDatanodes;

	public Daemon hbthread = null; // Heartbeatsender thread
	public Daemon commandsthread = null; // commands thread


	private int heartbeatInterval = Simulator.getHeartbeat();// in ms
	private int bandwidth		   = Simulator.getBandwidth();//in b/s
	private int blockSize			= Simulator.getBlockSize()*1000;//in B
	
	private long transferTime;
	//Communication recheck interval
	private long communicationRecheckInterval = Simulator.getHeartbeat()/2;
	//Communication hearbeatsender interval
	private long heartbeatRecheckInterval = Simulator.getHeartbeat()/5;

	// Lock for accessing the namenode command list
	private Object namenodeLock = new Object();

	// Lock for accessing the datanode command list
	private Object datanodeLock = new Object();

	boolean checkingPending = false;

	public AllDatanode() {

		datanodes = new ArrayList<Datanode>();
		toNamenode = Simulator.getToNamenode();
		toDatanodes = Simulator.getToDatanodes();

		this.hbthread = new Daemon(new HeartbeatSender());
		this.commandsthread = new Daemon(new CommandsHandler());

		this.bandwidth = Simulator.getBandwidth();

		this.transferTime = (blockSize * 8) / this.bandwidth;

	}

	void addNode(Datanode datanode) {
		datanodes.add(datanode);
	}

	Datanode getNode(int id) {
		return datanodes.get(id);
	}
	
	boolean killNode(int id){
		return getNode(id).kill();
	}

	int datanodeListSize() {
		return datanodes.size();
	}

	public boolean addToNamenodeQueue(Event e) {
		synchronized (namenodeLock) {
			// System.out.println("adding heartbeat to namenode...");
			boolean result = toNamenode.add(e);
			// System.out.println("added heartbeat to namenode.");
			return result;
		}
	}

	public boolean addToDatanodeQueue(Event e) {
		synchronized (datanodeLock) {
			// System.out.println("adding heartbeat to namenode...");
			boolean result = toDatanodes.add(e);
			// System.out.println("added heartbeat to namenode.");
			return result;
		}
	}

	/**
	 * Starts the datanodes management thread
	 */
	public void start() {
		isRunning = true;
		hbthread.start();
		commandsthread.start();
	}

	/**
	 * Stops the datanodes management thread
	 */
	public void stop() {
		isRunning = false;

		try {
			hbthread.join();
			commandsthread.join();

		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Send the heartbeats.
	 */
	class HeartbeatSender implements Runnable {

		public void run() {
			while (isRunning) {
				try {
					checkHearbeat();
					Thread.sleep(heartbeatRecheckInterval);
				} catch (Exception e) {
					// FSNamesystem.LOG.error(StringUtils.stringifyException(e));
				}
			}
		}

		private void checkHearbeat() {
			Datanode current;
			for (int i = 0; i < datanodes.size(); i++) {
				current = datanodes.get(i);
				if (!current.hasFailed()
						&& (Node.now() >= heartbeatInterval
								+ current.getLastHB())) {
					sendHeartbeat(i);
					// System.out.print("HB from " + current.getId() + "\n");
					current.setLastHB(Node.now());
				}
			}
		}
	}

	public boolean sendHeartbeat(int id) {
		return addToNamenodeQueue(new Event(id, Event.HEARTBEAT, Node.now()));
	}

	/**
	 * Process the commands received from NameNode
	 */
	class CommandsHandler implements Runnable {

		public void run() {
			while (isRunning) {
				try {
					handleCommands();
					Thread.sleep(communicationRecheckInterval);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		private void handleCommands() {
			while(!toDatanodes.isEmpty()){
				Event command = toDatanodes.poll();
	
				if (command != null) {
					// Handle it
					if (command.getAction() == Event.REPLICATION) {
						if (!handleTransfer(command.getSource(),
								command.getDestination(), command.getBlockId())) {
							toDatanodes.add(command);
						}
					} else if(command.getAction() == Event.PENDINGTRANSFER){
						if (!handleReception(command.getSource(),
								command.getDestination(), command.getBlockId())) {
							toDatanodes.add(command);
						}
					}
				}
			}
		}

		private boolean handleReception(int idSource, int idDestination, int blockId) {
			Datanode sourceNode = null;
			Datanode destinationNode = null;

			for (Datanode node : datanodes) {
				if (node.getId() == idSource) {
					sourceNode = node;
				} else if (node.getId() == idDestination) {
					destinationNode = node;
				}
			}
			boolean NodesAlive = (!sourceNode.hasFailed() && !destinationNode.hasFailed());
			
			if(Node.now() >= destinationNode.getDownloadingTime() && NodesAlive){
				//Send signal to Namenode that the transfer is finished
				toNamenode.add(new Event(destinationNode.getId(), Event.BLOCKRECEPTION, Node.now(),0,blockId));
				return true;
			} else if(!NodesAlive){
				sourceNode.setUploadingTime(Node.now());
				destinationNode.setDownloadingTime(Node.now());
				return true;
			} else{
				return false;
			}
		}

		public boolean handleTransfer(int idSource, int idDestination,
				int idBlock) {
			Datanode sourceNode = null;
			Datanode destinationNode = null;

			if(idSource != idDestination){
			
				int found=0;
				for (Datanode node: datanodes ) {
					if (node.getId() == idSource) {
						sourceNode = node;
						found++;
					} else if (node.getId() == idDestination) {
						destinationNode = node;
						found++;
					} else if(found == 2){
						break;
					}
				}
				
				if(found < 2){//We haven't found our nodes. (means that there is an error, so the pending request cannot succeed.
					return true;
				}
				
				boolean NodesAlive = (!sourceNode.hasFailed() && !destinationNode.hasFailed());
	
				if(NodesAlive){
				
					boolean ableToTransfer = (Node.now() > sourceNode
							.getUploadingTime())
							&& (Node.now() > destinationNode.getDownloadingTime());
		
					if ((sourceNode != null) && (destinationNode != null)
							&& ableToTransfer) {
						long time = Node.now() + transferTime;
						sourceNode.setUploadingTime(time);
						destinationNode.setDownloadingTime(time);
						
						toDatanodes.add(new Event(sourceNode.getId(), Event.PENDINGTRANSFER, 0L, destinationNode.getId(), idBlock));
						return true;
					} else if (!ableToTransfer) {
						return false;
					} else {
						return false;
					}
				}
			}
			return true;
		}
	}


	
}
