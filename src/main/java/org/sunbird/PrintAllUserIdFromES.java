package org.sunbird;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.http.HttpHeaders;
import org.sunbird.util.HttpClientUtil;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PrintAllUserIdFromES {

	private static ObjectMapper mapper = new ObjectMapper();
	private static String filename = System.nanoTime() + "_outputFile.txt";

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		String esip = args[0];
		String esport = args[1];
		String index = args[2];
		int size = 10000;

		String uri = "http://" + esip + ":" + esport + "/" + index + "/_search?scroll=1m&pretty";
		Map<String, Object> request = new HashMap<>();
		request.put("size", size);
		Map<String, Object> query = new HashMap<>();
		Map<String, Object> empty_query = new HashMap<>();
		query.put("match_all", empty_query);
		request.put("query", query);
		List<String> fields = new ArrayList<>();
		fields.add("id");
		request.put("_source", fields);
		Map<String, Object> response = post(request, uri);
		System.out.println("Response from first query : " + response);
		Map<String, Object> hits = (Map<String, Object>) response.get("hits");
		int totalRecords = (int) hits.get("total");
		if (MapUtils.isNotEmpty(hits)) {
			List<Map<String, Object>> hitsList = (List<Map<String, Object>>) hits.get("hits");
			hitsList.stream().forEach(hit -> {
				Map<String, Object> source = (Map<String, Object>) hit.get("_source");
				String userId = (String) source.get("id");

				// ---------------------------------------------------------------------------------------------------------
				// Write response to log file
				//System.out.println("Fetched userId : " + userId);
				try {
					FileWriter fw = new FileWriter(filename, true);
					fw.write(userId + "\n");
					fw.close();
				} catch (Exception ioe) {
					System.err.println("IOException: " + ioe.getMessage());
					ioe.printStackTrace();
				}
				// -------------------------------------------------------------------------------

			});
		}

		String _scroll_id = (String) response.get("_scroll_id");

		String scrolluri = "http://" + esip + ":" + esport + "/_search/scroll?pretty";
		int loopCount = (totalRecords / size);
		System.out.println("Total Records in ES : "+totalRecords);
		
		if (loopCount > 0) {
			for (int count = 1; count <= loopCount; count++) {
				Map<String, Object> scrollRequest = new HashMap<>();
				scrollRequest.put("scroll", "1m");
				scrollRequest.put("scroll_id", _scroll_id);
				Map<String, Object> scrollResponse = post(scrollRequest, scrolluri);
				System.out.println("Response from consecutive scroll query : "+ count + " : "+ scrollResponse);

				Map<String, Object> scrollHits = (Map<String, Object>) scrollResponse.get("hits");
				if (MapUtils.isNotEmpty(scrollHits)) {
					List<Map<String, Object>> hitsList = (List<Map<String, Object>>) scrollHits.get("hits");
					hitsList.stream().forEach(hit -> {
						Map<String, Object> source = (Map<String, Object>) hit.get("_source");
						String userId = (String) source.get("id");

						// ---------------------------------------------------------------------------------------------------------
						// Write response to log file
						//System.out.println("Fetched userId : " + userId);
						try {
							FileWriter fw = new FileWriter(filename, true);
							fw.write(userId + "\n");
							fw.close();
						} catch (Exception ioe) {
							System.err.println("IOException: " + ioe.getMessage());
							ioe.printStackTrace();
						}
						// -------------------------------------------------------------------------------
					});
				}

			}
		}
	}

	public static Map<String, Object> post(Map<String, Object> requestBody, String uri) {
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
