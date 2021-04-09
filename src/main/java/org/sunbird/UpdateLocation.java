package org.sunbird;

import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.sunbird.util.HttpClientUtil;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class UpdateLocation {
	private static AtomicInteger counter = new AtomicInteger(1);
	private static ObjectMapper mapper = new ObjectMapper();

	public static void main(String[] args) {
		String filePath = args[0];
		String learnerport = args[1];
		String learnerip = args[2];
		String dryRun = args[3];
		String NoOfThread = args[4];

		int totalThreads = Integer.parseInt(NoOfThread);
		List<String> userIdList = parseTextFile(filePath);
		System.out.println("Total user size from csv : " + userIdList.size());

		ExecutorService executor = Executors.newFixedThreadPool(totalThreads);
		String filename = System.nanoTime() + "_outputFile.txt";
		
		userIdList.stream().forEach(userId -> {
			executor.execute(() -> {
				Map<String, Object> userRequest = new HashMap<>();
				Map<String, Object> request = new HashMap<>();
				if (StringUtils.isNotBlank(dryRun)) {
					request.put("dryRun", Boolean.parseBoolean(dryRun));
				}
				List<String> userIds = new ArrayList<>();
				userIds.add(userId);
				request.put("userIds", userIds);
				userRequest.put("request", request);

				String uri = "http://" + learnerip + ":" + learnerport + "/v1/user/update/userlocation";
				Map<String, Object> response = patch(userRequest, uri);
				int count = counter.getAndIncrement();
				System.out.println("Response : " + count + " : "+ response);

				// ---------------------------------------------------------------------------------------------------------
				// Write response to log file
				try {
					FileWriter fw = new FileWriter(filename, true);
					fw.write("Response : " + count + " : for userId : "+userId +" : "+ mapper.writeValueAsString(response) + "\n");
					fw.write("\r\n");
					fw.close();
				} catch (Exception ioe) {
					System.err.println("IOException: " + ioe.getMessage());
					ioe.printStackTrace();
				}
				// -------------------------------------------------------------------------------
			});
		});

		// -----------------------------------------------------------------------------------------------------------

		executor.shutdown();
		// Wait until all threads are finish
		while (!executor.isTerminated()) {
			 
		}

		System.out.println("Finished all threads");

		// ------------------------------------------------------------------------------------------------------------

	}

	private static List<String> parseTextFile(String filePath) {
		List<String> userIds = new LinkedList<>();
		try {
			List<String> allLines = Files.readAllLines(Paths.get(filePath));
			for (String line : allLines) {
				userIds.add(line);
			}
		} catch (Exception e) {
			System.out.println("Exception occurred while reading file.");
			e.printStackTrace();
		}
		return userIds;
	}

	public static Map<String, Object> patch(Map<String, Object> requestBody, String uri) {
		try {

			HttpClientUtil client = HttpClientUtil.getInstance();
			Map<String, String> headers = new HashMap<>();
			headers.put(HttpHeaders.ACCEPT, "application/json");
			headers.put(HttpHeaders.CONTENT_TYPE, "application/json");
			String response = client.patch(uri, mapper.writeValueAsString(requestBody), headers);

			return mapper.readValue(response, new TypeReference<Map<String, Object>>() {
			});
		} catch (Exception ex) {
			System.out.println("Learner update user location patch call: Exception occurred = ");
			ex.printStackTrace();
		}
		return new HashMap<>();
	}

}
