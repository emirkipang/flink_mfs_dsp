package util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.json.JSONObject;

public class Helper {
	public static String isNull(String text) {
		return (text == null) ? "UNKNOWN" : text;
	}

	public static String combineFileds(int start, int end, String[] items,
			String delimiter) {
		String result = items[start];
		for (int i = start + 1; i <= end; i++) {
			result = result + delimiter + items[i];
		}

		return result;
	}

	public static String joinRule(String in, int length) {
		int gap = length - in.length();

		if (gap != 0) {
			for (int i = 1; i <= gap; i++) {
				in = "0" + in;
			}
		}

		return in;
	}
	
	public static String getLacci(String msisdn) throws Exception {
		String url = "http://10.250.200.217:8001/?oprid=getAll&msisdn="+msisdn+"&show=json";
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
		// optional default is GET
		con.setRequestMethod("GET");
		// add request header
		con.setRequestProperty("User-Agent", "Mozilla/5.0");
		int responseCode = con.getResponseCode();
		System.out.println("\nSending 'GET' request to URL : " + url);
		System.out.println("Response Code : " + responseCode);
		BufferedReader in = new BufferedReader(new InputStreamReader(
				con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();
		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();

		// print in String
		System.out.println(response.toString());

		// Read JSON response and print
		JSONObject myResponse = new JSONObject(response.toString());
		//System.out.println("result after Reading JSON Response");
		String resultCode = myResponse.getString("ResultCode");
		String lac = "0";
		String ci = "0";
		if (resultCode.equals("0")){
			lac = myResponse.getString("lac");
			ci = myResponse.getString("ci");
		}
		

//		System.out.println("lac : " + myResponse.getString("lac"));
//		System.out.println("ci : " + myResponse.getString("ci"));
		
		return lac + "|" + ci;
	}
}
