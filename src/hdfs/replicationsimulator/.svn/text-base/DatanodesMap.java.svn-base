package hdfs.replicationsimulator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

/*
 * Map to store information about datanodes
 */
public class DatanodesMap {
	private Map<Integer, DatanodeInfo> map;
	private List<DatanodeInfo> heartbeats;
	
	Random r;

	/*
	 * Constructor (Initialize the blocksMap)
	 */
	DatanodesMap(){
		this.map = new HashMap<Integer, DatanodeInfo>();
		this.heartbeats = new ArrayList<DatanodeInfo>();
		r = new Random();
	}
	
	
	/*
	 * Add a node to the map
	 */
	public void addDatanodeInfo(DatanodeInfo DatanodeInfo){
		
		map.put(DatanodeInfo.getId(), DatanodeInfo);
		heartbeats.add(DatanodeInfo);
		//TODO
	}

	/*
	 * get a node from the map
	 */
	public DatanodeInfo getDatanodeInfo(int id){
		
		return map.get(id);
	}


	public List<DatanodeInfo> getHeartbeats() {
		return heartbeats;
	}


	public void removeDatanode(DatanodeInfo nodeInfo) {
		map.remove(nodeInfo);
		heartbeats.remove(nodeInfo);
	}
	
	public DatanodeInfo randomNode() {
		
		int nodeId = r.nextInt(heartbeats.size());
		
		return getDatanodeInfo(nodeId);
		
	}


	
}
