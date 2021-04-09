package org.sunbird;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FixUserLookupData {
	private static CassandraConnection connection;

	public static void main(String[] args) {
		String filePath = args[0];
		String cassandraIp = args[1];
		String keyspace = args[2];
		String dryRun = args[3];

		connection = CassandraConnection.getInstance(cassandraIp);

		List<Map<String, String>> userList = parseJsonFile(filePath);
		deleteUserLookupEntry(userList, keyspace, dryRun);
		connection.close();

	}

	private static List<Map<String, String>> parseJsonFile(String filePath) {

		// This will reference one line at a time
		String line = null;

		try {
			// FileReader reads text files in the default encoding.
			FileReader fileReader = new FileReader(filePath);
			List<Map<String, String>> userList = new ArrayList<>();
			// Always wrap FileReader in BufferedReader.
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			while ((line = bufferedReader.readLine()) != null) {
				JSONObject obj = (JSONObject) new JSONParser().parse(line);

				Set s = obj.entrySet();
				Iterator iter = s.iterator();

				Map hm = new HashMap();

				while (iter.hasNext()) {
					Map.Entry me = (Map.Entry) iter.next();
					if (null != (me.getKey())) {
						hm.put(me.getKey(), me.getValue());
					}
				}
				userList.add(hm);
			}
			// close files.
			bufferedReader.close();
			System.out.println("Successfully read the file.");
			System.out.println("Size of file:  " + userList.size());
			return userList;
		} catch (Exception ex) {
			System.out.println("Unable to open file '" + filePath + "'");
		}
		return null;

	}

	private static void deleteUserLookupEntry(List<Map<String, String>> userList, String keyspace, String dryRun) {
		boolean run = true;
		if (StringUtils.isNotBlank(dryRun)) {
			run = Boolean.parseBoolean(dryRun);
		} else {
			run = true;
		}

		List<Map<String, String>> logList = new ArrayList<>();
		for (Map<String, String> user : userList) {
			Map<String, String> logMap = new HashMap<>();
			try {
				String userId = user.get("userid");
				String encValue = user.get("value");
				String type = user.get("type");
				// ------------------------------------------------------------------------------------------------------------
				if (StringUtils.isNotBlank(userId) && StringUtils.isNotBlank(encValue)
						&& StringUtils.isNotBlank(type)) {
					String typeValue = null;
					String prevusedTypeValue = null;
					try {
						String query = "select * from " + keyspace + ".user where id = '" + userId + "';";
						ResultSet rs1 = connection.getSession().execute(query);
						Row r = rs1.one();
						typeValue = r.getString(type);
						String prevusedType = "prevused" + type;
						prevusedTypeValue = r.getString(prevusedType);
						String userQueryRes = "user record for userid  :" + userId + ", " + type + " = " + typeValue
								+ " & " + prevusedType + " = " + prevusedTypeValue;
						System.out.println(userQueryRes);
						logMap.put("userQueryResonse", userQueryRes);
					} catch (Exception ex) {
						ex.printStackTrace();
						logMap.put("UserReadException", ex.getMessage());
					}

					// --------------------------------------------------------------------------------------------------------------

					if (StringUtils.isNotBlank(typeValue)) {
						if (!(typeValue.equalsIgnoreCase(encValue))) {
							Map<String, Object> compositeKeyMap = new LinkedHashMap<>();
							compositeKeyMap.put("type", user.get("type"));
							compositeKeyMap.put("value", user.get("value"));

							Delete delete = null;
							try {
								delete = QueryBuilder.delete().from(keyspace, "user_lookup");
								Delete.Where deleteWhere = delete.where();
								compositeKeyMap.entrySet().stream().forEach(x -> {
									Clause clause = QueryBuilder.eq(x.getKey(), x.getValue());
									deleteWhere.and(clause);
								});
								String deleteQuery = delete.getQueryString();
								logMap.put("userLookupDeleteQuery", deleteQuery);
								System.out.println("Delete Query String: " + deleteQuery);
								if (!run) {
									ResultSet rst = connection.getSession().execute(delete);
									System.out.println("Delete query executed : " + rst.isExhausted());
								}
							} catch (Exception e) {
								e.printStackTrace();
								logMap.put("userLookupDeleteException", e.getMessage());
							}
						}
					} else {
						if (StringUtils.isBlank(typeValue)) {
							if (StringUtils.isNotBlank(prevusedTypeValue)
									&& prevusedTypeValue.equalsIgnoreCase(encValue)) {
								Map<String, Object> compositeKeyMap = new LinkedHashMap<>();
								compositeKeyMap.put("type", user.get("type"));
								compositeKeyMap.put("value", user.get("value"));
								String query = "Deleting entry form lookup table for Identifier freeup account for type ="+ user.get("type")+ " & value = "+user.get("value");
								logMap.put("userLookupDeleteOpForFreeupIdentifier", query);
								Delete delete = null;
								try {
									delete = QueryBuilder.delete().from(keyspace, "user_lookup");
									Delete.Where deleteWhere = delete.where();
									compositeKeyMap.entrySet().stream().forEach(x -> {
										Clause clause = QueryBuilder.eq(x.getKey(), x.getValue());
										deleteWhere.and(clause);
									});
									String deleteQuery = delete.getQueryString();
									logMap.put("userLookupDeleteQueryForFreeupIdentifier", deleteQuery);
									System.out.println("Delete Query String for freeup account: " + deleteQuery);
									if (!run) {
										ResultSet rst = connection.getSession().execute(delete);
										System.out.println("Delete query executed for freeup account: " + rst.isExhausted());
									}
								} catch (Exception e) {
									e.printStackTrace();
									logMap.put("userLookupDeleteExceptionForFreeupIdentifier", e.getMessage());
								}
							}
						}
					}
				}
			} catch (Exception ex) {
				ex.printStackTrace();
				logMap.put("Exception ", ex.getMessage());
			} finally {
				logList.add(logMap);
			}
		}
		// -----------------------------------------------------------------------------------------
		try {
			ObjectMapper mapper = new ObjectMapper();
			FileWriter myWriter = new FileWriter(System.nanoTime() + "_outputFile.txt");

			myWriter.write(mapper.writeValueAsString(logList));
			myWriter.close();
			System.out.println("Successfully wrote to the log file.");
		} catch (Exception e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}

	}

}
