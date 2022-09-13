package org.insight;


import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONObject;


/**
 * 
 * @author Radhya Sahal
 * @email radhya.sahal@gmail.com
 * 2020
 * This program to find regioin from GPS points 
 */

public class Get_Region_Name {
	
	public static void main(String[] args) {
		
		String lat="53.290075";
		String lon="-9.0743801";
		System.out.println(Get_Region_Name.Post_JSON(lat,lon));
	}

	
	public static String Post_JSON(String lat, String lon ) {
		//logger.log()
           	
		   //String query_url = "https://nominatim.openstreetmap.ie/reverse?format=jsonv2&lat=53.290075&lon=-9.0743801";
		 String url_str="https://nominatim.openstreetmap.ie/reverse?format=jsonv2&";
		  
		 String query_url = url_str+"lat="+lat+"&lon="+lon;

		   
		 String  regoin="";
			
         String json=""; 
           try {
			  
           URL url = new URL(query_url);
           HttpURLConnection conn = (HttpURLConnection) url.openConnection();
           conn.setConnectTimeout(5000);
           conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
           conn.setDoOutput(true);
           conn.setDoInput(true);
           conn.setRequestMethod("POST");

           OutputStream os = conn.getOutputStream();
           os.write(json.getBytes("UTF-8"));
           os.close(); 

           // read the response
           InputStream in = new BufferedInputStream(conn.getInputStream());
           String result = IOUtils.toString(in, "UTF-8");
          

          // System.out.println(result);
           
           System.out.println("result after Reading JSON Response");
           
           
           
           JSONObject myResponse = new JSONObject(result);
           JSONObject myResponseadress =myResponse.getJSONObject("address");
           regoin=myResponseadress.getString("city_district");
          // System.out.println("place_id:"+myResponse.getString("place_id"));
           System.out.println("Regoin:"+regoin);
           //regoin="\""+regoin+"\"";
         
           
           // System.out.println("id- "+myResponse.getInt("id"));
         // System.out.println("result- "+myResponse.getString("result"));
           
         //  regoin="{\"regoin\":\""+regoin+"\"}";
         //  ObjectMapper mapper = new ObjectMapper();
		  // JsonNode regoinNode = mapper.readTree(regoin);
		 //  System.out.println(regoinNode.get("regoin"));
           in.close();
          
           conn.disconnect();

           
	
           } catch (Exception e) {
   			System.out.println(e);
   		}
	 
           
           
           return regoin;
	}
	
	
	
}
