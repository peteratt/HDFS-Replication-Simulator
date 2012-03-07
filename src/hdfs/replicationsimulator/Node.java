package hdfs.replicationsimulator;
/**
 * 
 * Define the general methods for a node (implemented by Datanode & NameNode)
 * 
 * @author peteratt
 * @version 0.1
 */


public class Node {
	 	/**
	   * Current system time.
	   * @return current time in msec.
	   */
	  static long now() {
	    return System.currentTimeMillis();
	  }
}
