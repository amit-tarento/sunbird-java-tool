package org.sunbird;

import java.io.FileWriter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ExecutorTest {

	public static void main(String[] args) {
		ObjectMapper mapper = new ObjectMapper();
		ExecutorService executor = Executors.newFixedThreadPool(100);
		String filename = System.nanoTime() + "_outputFile.txt";

		for (int i = 0; i < 10000; i++) {
			final int x = i;
			executor.execute(() -> {

				try {
					if (x % 2 == 0) {
						Thread.sleep(5000);
					} else {
						Thread.sleep(3000);
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				// ---------------------------------------------------------------------------------------------------------
				// Write response to log file
				try {
					FileWriter fw = new FileWriter(filename, true);
					fw.write(mapper.writeValueAsString("response") + "\n");
					fw.write("\r\n");
					fw.close();
				} catch (Exception ioe) {
					System.err.println("IOException: " + ioe.getMessage());
					ioe.printStackTrace();
				}
				System.out.println("Task " + x);
				// -------------------------------------------------------------------------------
			});
		}

		executor.shutdown();
		// Wait until all threads are finish
		while (!executor.isTerminated()) {

		}
		System.out.println("Finished all threads");

	}

}
