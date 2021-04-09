package org.sunbird.util;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.MapUtils;
import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;

public class HttpClientUtil {

	private static CloseableHttpClient httpclient = null;
	private static HttpClientUtil httpClientUtil;

	private HttpClientUtil() {
		ConnectionKeepAliveStrategy keepAliveStrategy = (response, context) -> {
			HeaderElementIterator it = new BasicHeaderElementIterator(response.headerIterator(HTTP.CONN_KEEP_ALIVE));
			while (it.hasNext()) {
				HeaderElement he = it.nextElement();
				String param = he.getName();
				String value = he.getValue();
				if (value != null && param.equalsIgnoreCase("timeout")) {
					return Long.parseLong(value) * 1000;
				}
			}
			return 180 * 1000;
		};

		PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
		connectionManager.setMaxTotal(200);
		connectionManager.setDefaultMaxPerRoute(150);
		connectionManager.closeIdleConnections(180, TimeUnit.SECONDS);
		httpclient = HttpClients.custom().setConnectionManager(connectionManager).useSystemProperties()
				.setKeepAliveStrategy(keepAliveStrategy).build();
	}
	
	public static HttpClientUtil getInstance() {
	    if (httpClientUtil == null) {
	      synchronized (HttpClientUtil.class) {
	        if (httpClientUtil == null) {
	        	httpClientUtil = new HttpClientUtil();
	        }
	      }
	    }
	    return httpClientUtil;
	  }

	public String patch(String requestURL, String params, Map<String, String> headers) {
		CloseableHttpResponse response = null;
		try {
			HttpPatch httpPatch = new HttpPatch(requestURL);
			if (MapUtils.isNotEmpty(headers)) {
				for (Map.Entry<String, String> entry : headers.entrySet()) {
					httpPatch.addHeader(entry.getKey(), entry.getValue());
				}
			}
			StringEntity entity = new StringEntity(params);
			httpPatch.setEntity(entity);

			response = httpclient.execute(httpPatch);
			int status = response.getStatusLine().getStatusCode();
			if (status >= 200 && status < 300) {
				HttpEntity httpEntity = response.getEntity();
				byte[] bytes = EntityUtils.toByteArray(httpEntity);
				StatusLine sl = response.getStatusLine();
				System.out.println("Response from patch call : " + sl.getStatusCode() + " - " + sl.getReasonPhrase());
				return new String(bytes);
			} else {
				return "";
			}
		} catch (Exception ex) {
			System.out.println("Exception occurred while calling patch method");
			ex.printStackTrace();
			return "";
		} finally {
			if (null != response) {
				try {
					response.close();
				} catch (Exception ex) {
					System.out.println("Exception occurred while closing patch response object");
				}
			}
		}
	}
	
	public String post(String requestURL, String params, Map<String, String> headers) {
		CloseableHttpResponse response = null;
		try {
			HttpPost httpPost = new HttpPost(requestURL);
			if (MapUtils.isNotEmpty(headers)) {
				for (Map.Entry<String, String> entry : headers.entrySet()) {
					httpPost.addHeader(entry.getKey(), entry.getValue());
				}
			}
			StringEntity entity = new StringEntity(params);
			httpPost.setEntity(entity);

			response = httpclient.execute(httpPost);
			int status = response.getStatusLine().getStatusCode();
			if (status >= 200 && status < 300) {
				HttpEntity httpEntity = response.getEntity();
				byte[] bytes = EntityUtils.toByteArray(httpEntity);
				StatusLine sl = response.getStatusLine();
				System.out.println("Response from post call : " + sl.getStatusCode() + " - " + sl.getReasonPhrase());
				return new String(bytes);
			} else {
				return "";
			}
		} catch (Exception ex) {
			System.out.println("Exception occurred while calling post method");
			ex.printStackTrace();
			return "";
		} finally {
			if (null != response) {
				try {
					response.close();
				} catch (Exception ex) {
					System.out.println("Exception occurred while closing post response object");
				}
			}
		}
	}

}
