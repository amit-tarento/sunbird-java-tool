package org.sunbird;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.lang3.StringUtils;
import org.sunbird.util.encryption.BASE64Decoder;
import org.sunbird.util.encryption.BASE64Encoder;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;

public class EncryptionUtil {

	private static String ALGORITHM = "AES";
	private static int ITERATIONS = 3;
	private static byte[] keyValue = new byte[] { 'T', 'h', 'i', 's', 'A', 's', 'I', 'S', 'e', 'r', 'c', 'e', 'K', 't',
			'e', 'y' };
	

	public static void main(String[] args) throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException,
			IllegalBlockSizeException, BadPaddingException, UnsupportedEncodingException {
		String encryptionKey = args[0];
		String filePath = args[1];

		Set<String> phoneList = getDataMapList(parsecsv(filePath));
		System.out.println("Complete phone list :  "+phoneList.size());
		System.out.println("Phone list : "+ phoneList);
		encryptList(phoneList, encryptionKey);
		

	}

	private static void printUserLookupDetails(String type, String value) {
		CassandraConnection connection = CassandraConnection.getInstance("");
		String query = "select * from sunbird.user_lookup where type = '" + type + "' and value = '"+value+"';";
		System.out.println("Query :   "+query);
		ResultSet rs =
		        connection.getSession().execute(query);
		Row r = rs.one();
		System.out.println(r.getString("type"));
		System.out.println(r.getString("value"));
		System.out.println(r.getString("userid"));
		  	
	}

	private static Set<String> getDataMapList(List<String[]> dataList) {
		Set<String> data = new HashSet<>();
		if (dataList.size() > 1) {
			try {
				String[] columnArr = dataList.get(0);
				columnArr = trimColumnAttributes(columnArr);
				for (int i = 1; i < dataList.size(); i++) {
					String[] valueArr = dataList.get(i);
					for (int j = 0; j < valueArr.length; j++) {
						String value = (valueArr[j].trim().length() == 0 ? null : valueArr[j].trim());
						data.add(value);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return data;
	}

	private static List<String[]> parsecsv(String filePath) {
		CSVReader csvReader = null;
		List<String[]> rows = new ArrayList<>();
		try {
			// parsing a CSV file into CSVReader class constructor
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
		} catch (Exception e) {
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

	public static String decrypt(String value, String encryptionKey) {
		Cipher c = null;
		try {
			Key key = generateKey();
			c = Cipher.getInstance(ALGORITHM);
			c.init(Cipher.DECRYPT_MODE, key);
		} catch (Exception e) {
			System.out.println("Exception occurred " + e.getMessage());
			e.printStackTrace();
		}
		try {
			String dValue = null;
			String valueToDecrypt = value.trim();
			for (int i = 0; i < ITERATIONS; i++) {
				byte[] decordedValue = new BASE64Decoder().decodeBuffer(valueToDecrypt);
				byte[] decValue = c.doFinal(decordedValue);
				dValue = new String(decValue, StandardCharsets.UTF_8).substring(encryptionKey.length());
				valueToDecrypt = dValue;
			}
			return dValue;
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return value;
	}

	public static void encryptList(Set<String> valueList, String encryptionKey)
			throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException, IllegalBlockSizeException,
			BadPaddingException, UnsupportedEncodingException {
	
		List<Map<String,String>> resList = new ArrayList<>();
		for (String value : valueList) {
			if (StringUtils.isNotBlank(value)) {
				Map<String, String> resMap = new HashMap<>();
				resMap.put("phone", value);
				String encValue = encrypt(value, encryptionKey);
				resMap.put("encValue", encValue);
				resList.add(resMap);
				printUserLookupDetails("phone",encValue);
				
			}
		}

		try {
			ObjectMapper mapper = new ObjectMapper();
			FileWriter myWriter = new FileWriter(System.nanoTime() + "_outputFile.txt");

			myWriter.write(mapper.writeValueAsString(resList));
			myWriter.close();
			System.out.println("Successfully wrote to the file.");
		} catch (Exception e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
	}

	public static String encrypt(String value, String encryptionKey)
			throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException,
			BadPaddingException, UnsupportedEncodingException {
		Cipher c = null;
		try {
			Key key = generateKey();
			c = Cipher.getInstance(ALGORITHM);
			c.init(Cipher.ENCRYPT_MODE, key);
		} catch (Exception e) {
			System.out.println("Exception occurred " + e.getMessage());
			e.printStackTrace();
		}
		String valueToEnc = null;
		String eValue = value;
		for (int i = 0; i < ITERATIONS; i++) {
			valueToEnc = encryptionKey + eValue;
			byte[] encValue = c.doFinal(valueToEnc.getBytes(StandardCharsets.UTF_8));
			eValue = new BASE64Encoder().encode(encValue);
		}
		return eValue;
	}

	private static Key generateKey() {
		return new SecretKeySpec(keyValue, ALGORITHM);
	}

}
