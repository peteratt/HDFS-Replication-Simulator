package hdfs.replicationsimulator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class BlockInfo extends Block implements  Comparable<BlockInfo> {

	private List<DatanodeInfo> datanodes;
	
	BlockInfo(int id) {
		super(id);
		datanodes = new ArrayList<DatanodeInfo>();
	}

	/**
	 * Add a node to the list of storage nodes
	 */
	public void addDatanode(DatanodeInfo dn) {
		datanodes.add(dn);
	}
	
	/**
	 * Remove a node to the list of storage nodes
	 */
	boolean removeDataNode(DatanodeInfo DatanodeInfo) {
		return datanodes.remove(DatanodeInfo);
	}
	
	boolean hasFailed() {
		return datanodes.isEmpty();
	}
	
	int numberOfReplicas() {
		return datanodes.size();
	}
	
	public Iterator<DatanodeInfo> nodeIterator() {
		return datanodes.iterator();
	}
	
	/** {@inheritDoc} */
	  public int compareTo(BlockInfo b) {
	   if(b.getId() > this.getId()){
		   return 1;
	   } else {
		   return -1;
	   }
	  }
	
	 public List<DatanodeInfo> getContainingNodes(){
		 return datanodes;
	 }
	
}
