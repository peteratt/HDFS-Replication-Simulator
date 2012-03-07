package hdfs.replicationsimulator;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SimFileGenerator {

	// Types:
	// 0 is for constant
	// 1 is for exponential
	// 2 is for gaussian
	private static final int CONSTANT = 0;
	private static final int EXPONENTIAL = 1;
	private static final int GAUSSIAN = 2;

	private static String filename;

	// Defaults
	private static int distrType = 0;
	private static int mttf = 30000;

	private static int time;
	private static int nodes;
	private static int failingNodes;
	private static int nBlocks;

	private static List<Integer> failedNodes;

	public static void main(String[] args) {
		try {
			parseArgs(args);
		} catch (Exception e) {
			System.err.println("Error while parsing parameters");
			System.exit(1);
		}
	}

	private static void parseArgs(String[] args) throws Exception {

		for (String s : args) {
			if (s.startsWith("file=")) {
				filename = s.split("=")[1];
				System.out.println("File is: " + filename);
			}
		}

		File f = new File(filename);
		FileWriter fw = new FileWriter(f);

		for (String s : args) {
			if (s.startsWith("replica=")) {
				fw.write(s + "\n");
				System.out.println(s + "\n");
			} else if (s.startsWith("bw=")) {
				fw.write(s + "\n");
			} else if (s.startsWith("block=")) {
				fw.write(s + "\n");
			} else if (s.startsWith("heartbeat=")) {
				fw.write(s + "\n");
			} else if (s.startsWith("timeout=")) {
				fw.write(s + "\n");
			} else if (s.startsWith("MTTF=")) {
				mttf = Integer.parseInt(s.split("=")[1]);
			} else if (s.startsWith("distr=")) {
				distrType = Integer.parseInt(s.split("=")[1]);
			} else if (s.startsWith("nBlocks=")) {
				fw.write(s + "\n");
			} else if (s.startsWith("nodes=")) {
				fw.write(s + "\n");
				nodes = Integer.parseInt(s.split("=")[1]);
			} else if (s.startsWith("failPercent=")) {
				int expectedFailures = nodes
						* Integer.parseInt(s.split("=")[1]) / 100;
				failingNodes = poisson(expectedFailures);
			}

		}

		int failed = 0; // Milliseconds

		failedNodes = new ArrayList<Integer>();

		while (failed <= failingNodes) {
			fw.write("failure_time=" + nextFailure() + "\n");
			fw.write("failing_node_id=" + nextFailingNode() + "\n");
			failed++;
		}

		fw.close();
	}

	private static String nextFailingNode() {
		Random r = new Random();
		boolean failed = true;
		int failingId = 1;
		while (failed) {
			failingId = r.nextInt(nodes) + 1;
			failed = false;

			for (Integer id : failedNodes) {
				if (id == failingId) {
					failed = true;
				}
			}
			if (!failed) {
				failedNodes.add(failingId);
			}
		}
		return "" + failingId;
	}

	/**
	 * Calculates next failure time
	 * 
	 * @return
	 */
	private static int nextFailure() {
		int timeDelta = 0;

		switch (distrType) {
		case CONSTANT:
			timeDelta = constant();
			break;

		case EXPONENTIAL:
			timeDelta = exponential();
			break;

		case GAUSSIAN:
			timeDelta = gaussian();
			break;
		}

		time += timeDelta;

		return time;
	}

	/**
	 * Return a real number from an exponential distribution with rate lambda.
	 */
	public static int constant() {
		return mttf;
	}

	/**
	 * Return a real number from an exponential distribution with rate lambda.
	 */
	public static int exponential() {
		Random r = new Random();
		return (int) Math.round(-Math.log(1 - r.nextDouble()) * mttf);
	}

	/**
	 * Return a real number with a standard Gaussian distribution.
	 */
	public static int gaussian() {
		// use the polar form of the Box-Muller transform
		double r, x, y;
		do {
			x = uniform(-1.0, 1.0);
			y = uniform(-1.0, 1.0);
			r = x * x + y * y;
		} while (r >= 1 || r == 0);
		return (int) Math.round(x * Math.sqrt(-2 * Math.log(r) / r));

		// Remark: y * Math.sqrt(-2 * Math.log(r) / r)
		// is an independent random gaussian
	}

	/**
	 * Return real number uniformly in [a, b).
	 */
	public static double uniform(double a, double b) {
		Random r = new Random();
		return a + r.nextDouble() * (b - a);
	}

	/**
	 * Return an integer from a poisson distribution with expected failures
	 * given as parameter.
	 */
	public static int poisson(int expectedFailures) {
		double l = Math.exp(-expectedFailures);
		int k = 0;
		double p = 1;
		Random r = new Random();
		do {
			k++;
			double u = r.nextDouble();
			p *= u;
		} while (p > l);
		int nFailures = k - 1;
		return nFailures;
	}

}
