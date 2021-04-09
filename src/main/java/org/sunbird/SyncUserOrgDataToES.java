package org.sunbird;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.http.HttpHeaders;
import org.sunbird.util.HttpClientUtil;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SyncUserOrgDataToES {
	
	private static FileWriter myWriter = null;
	private static  BufferedWriter bw = null;

	public static void main(String[] args) throws IOException {
		long startTime = System.currentTimeMillis();
		myWriter = new FileWriter(System.nanoTime() + "_outputFile.txt", true);
		bw = new BufferedWriter(myWriter);
		String filePath = args[0];
		String learnerport = args[1];
		String learnerip = args[2];
		String batchCount = args[3];
		String objectType = args[4];
		String dryRun = args[5];

	
		int count = Integer.parseInt(batchCount);
		int loop = count;
		System.out.println("Sync started for objectType : " + objectType);
		LineIterator it = FileUtils.lineIterator(new File(filePath), "UTF-8");
		try {
			List<String> ids = new ArrayList<>(count);
			while (it.hasNext()) {
				if (loop > 0) {
					ids.add(it.nextLine().trim());
					loop--;
				} else {
					sync(ids, learnerip, learnerport, objectType, dryRun);
					loop = count;
					ids.clear();
					ids.add(it.nextLine().trim());
					loop--;
				}
			}
			sync(ids, learnerip, learnerport, objectType, dryRun);
		} catch(Exception ex) {
			System.out.println("Exception occurred while sync");
			ex.printStackTrace();
		} finally {
			LineIterator.closeQuietly(it);
			long endTime = System.currentTimeMillis();
			long totalTimeTaken = endTime - startTime;
			System.out.println("Total Time taken to sync = "+totalTimeTaken);
			bw.close();
		}
		
	}

	private static void sync(List<String> ids, String learnerip, String learnerport, String objectType, String dryRun) {
		long startTime = System.currentTimeMillis();
		Map<String, Object> response = new HashMap<String, Object>();
		if (ids.size() > 0) {
			System.out.println("total ids to sync :  " + ids.size() + " : " + ids);
			String uri = "http://" + learnerip + ":" + learnerport + "/v1/data/sync";
			Map<String, Object> req = new HashMap<>();
			Map<String, Object> request = new HashMap<>();
			request.put("objectIds", ids);
			request.put("objectType", objectType);
			request.put("operationType","sync");
			req.put("request", request);
			response = post(req, uri, dryRun);
			System.out.println("Response : " + response);
		}
		long endTime = System.currentTimeMillis();
		long totalTimeTaken = endTime - startTime;
		System.out.println("Total Time taken to sync these ids := "+ ids +", Time Taken: " +totalTimeTaken);
	}

	public static Map<String, Object> post(Map<String, Object> requestBody, String uri, String dryRun) {
		try {
			ObjectMapper mapper = new ObjectMapper();
			HttpClientUtil client = HttpClientUtil.getInstance();
			Map<String, String> headers = new HashMap<>();
			headers.put(HttpHeaders.ACCEPT, "application/json");
			headers.put(HttpHeaders.CONTENT_TYPE, "application/json");
			Boolean bool = Boolean.parseBoolean(dryRun);
			String reqBody = mapper.writeValueAsString(requestBody);
			System.out.println("RequestBody for sync api : "+reqBody);
			if (!bool) {
				System.out.println("Learner sync api called.");
				String response = client.post(uri, reqBody, headers);
				System.out.println("Learner sync api response."+response);
				// -----------------------------------------------------------------------------------------
				try {
			         bw.write("RequestBody for sync api : "+reqBody);
			         bw.write("\n\n");
			         bw.write("ResponseBody for sync api : "+response);
			         bw.write("\n\n");
					 System.out.println("Successfully wrote to the log file.");
				} catch (Exception e) {
					System.out.println("An error occurred while writting log file.");
					e.printStackTrace();
				}
				//-------------------------------------------------------------------------------------------
				return mapper.readValue(response, new TypeReference<Map<String, Object>>() {
				});
			}
		} catch (Exception ex) {
			System.out.println("Learner sync api call: Exception occurred = ");
			ex.printStackTrace();
		}
		return new HashMap<>();
	}

}
