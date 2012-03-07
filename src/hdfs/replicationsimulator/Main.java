package hdfs.replicationsimulator;
/**
 * 
 * Launches the simulator
 * 
 * @author peteratt
 * @version 0.1
 */


public class Main {

	/**
	 * @param args
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException {
		String configFile;
		if (args.length < 1) {
			configFile = "config.txt";
		} else {
			configFile = args[0];
		}
		Simulator.init(configFile);
		Simulator.start();
		
//		while (!isEnded) wait();
		Thread.sleep(1200000);
		System.out.println("END");
		
		Simulator.printResults();
		System.exit(0);
	}
	
}
