package org.sunbird;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.sunbird.util.HttpClientUtil;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class UpdateUserName {

  private static ObjectMapper mapper = new ObjectMapper();
	
  public static void main(String[] args) {
    String filePath = args[0];
    String learnerport = args[1];
    String learnerip = args[2];
    String dryRun = args[3];

    List<String[]> rows = parsecsv(filePath);
    List<Map<String,String>> userList = getUserMapList(rows);
    System.out.println("Total user size from csv : "+userList.size());
  
    List<String> resList = new ArrayList<>();
    long start = System.currentTimeMillis();
    userList.stream().forEach( user -> {
        Map<String, Object> userRequest = new HashMap<>();
        Map<String, Object> request = new HashMap<>();
        if (StringUtils.isNotBlank(dryRun)) {
          request.put("dryRun",Boolean.parseBoolean(dryRun));
        }
        List<String> userIds = new ArrayList<>();
        userIds.add(user.get("id"));
        request.put("userIds", userIds);
        userRequest.put("request", request);
       
        String uri = "http://"+learnerip+":"+learnerport+"/v1/user/update/username";
        Map<String, Object>  response = patch(userRequest,uri);
        System.out.println("Response : "+response);
        
        try {
			resList.add(mapper.writeValueAsString(response));
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
      });
    
    System.out.println("Total time taken to complete : "+(System.currentTimeMillis()-start));
    
    try {
        ObjectMapper mapper = new ObjectMapper();
        FileWriter myWriter = new FileWriter(System.nanoTime()+"_outputFile.txt");
       
        myWriter.write(mapper.writeValueAsString(resList));
        myWriter.close();
        System.out.println("Successfully wrote to the file.");
      } catch (IOException e) {
        System.out.println("An error occurred.");
        e.printStackTrace();
      }
  }

  public static Map<String, Object> patch(Map<String, Object> requestBody, String uri ) {
    try {
     
      HttpClientUtil client = HttpClientUtil.getInstance();
      Map<String, String> headers = new HashMap<>();
      headers.put(HttpHeaders.ACCEPT, "application/json");
      headers.put(HttpHeaders.CONTENT_TYPE, "application/json");
      String response = client.patch(uri, mapper.writeValueAsString(requestBody), headers);
     
      return mapper.readValue(response,
        new TypeReference<Map<String, Object>>() {});
    }catch (Exception ex) {
      System.out.println("Learner update username patch call: Exception occurred = ");
      ex.printStackTrace();
    }
    return new HashMap<>();
  }

  private static List<Map<String, String>> getUserMapList(List<String[]> dataList) {
    List<Map<String, String>> dataMapList = new ArrayList<>();
    if (dataList.size() > 1) {
      try {
        String[] columnArr = dataList.get(0);
        columnArr = trimColumnAttributes(columnArr);
        Map<String, String> dataMap = null;

        for (int i = 1; i < dataList.size(); i++) {
          dataMap = new HashMap<>(3);
          String[] valueArr = dataList.get(i);
          for (int j = 0; j < valueArr.length; j++) {
            String value = (valueArr[j].trim().length() == 0 ? null : valueArr[j].trim());
            String column = columnArr[j];
            if (StringUtils.isNotBlank(column) && (column.equalsIgnoreCase("id") || column.equalsIgnoreCase("channel"))) {
              dataMap.put(column, value);
            }
          }
          if (MapUtils.isNotEmpty(dataMap)) {
            dataMapList.add(dataMap);
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return dataMapList;
  }

  private static List<String[]> parsecsv(String filePath) {
    CSVReader csvReader = null;
    List<String[]> rows = new ArrayList<>();
    try
    {
      //parsing a CSV file into CSVReader class constructor
      csvReader = new CSVReader(new FileReader(filePath));
      String[] strArray;
      // Read one line at a time
      while ((strArray = csvReader.readNext()) != null) {
        List<String> list = new ArrayList<>();
        for (String token : strArray) {
          list.add(token);
        }
        rows.add(list.toArray(list.toArray(new String[strArray.length])));
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    return rows;
  }

  public static String[] trimColumnAttributes(String[] columnArr) {
    for (int i = 0; i < columnArr.length; i++) {
      columnArr[i] = columnArr[i].trim();
    }
    return columnArr;
  }

}
