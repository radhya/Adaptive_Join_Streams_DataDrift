package org.insight;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
//import org.insight.BeijingTrajectoryFL1_4.UserLocationSchema;

/* @ Radhya Sahal
 * This program to subscribe data from Kafka and then
 * join any two streams including sensor data and GPS 
 * 
 * using these agrgumments 
 Sever 90
 --topic android-sensor --bootstrap.servers 140.203.155.187:8017,140.203.155.187:8018,140.203.155.187:8019 
 --zookeeper.connect 140.203.155.187:8020 --group.id testgroupid2  --desTopic joinedstreams --WindowSize 2 --WindowType Session --JobName AdaptiveJoin
 
 local host

--topic android-sensor --bootstrap.servers localhost:9092  --zookeeper.connect localhost:2181 --group.id test-consumer-group2 --desTopic joinedstreams --WindowSize 2 --WindowType Tumble --JobName AdaptiveJoin
 
  
  
  Note: the mobile app should registered with use account
 * in case of not, Flink can't map tuples with null account which causes  NullPointerException 
 * pack flink app with dependencies
 * mvn clean compile assembly:single
 * http://www.baeldung.com/executable-jar-with-maven

 */
public class FlinkJoinSensorsStreamData {
	private static final Logger logger = Logger.getGlobal();

	
	private final static AscendingTimestampExtractor gpsTimeExtractor = new AscendingTimestampExtractor<Tuple7<String, String, String, String, Float, Float, Timestamp>>() {
		@Override
		public long extractAscendingTimestamp(Tuple7<String, String, String, String, Float, Float, Timestamp> node) {
			String strTime = node.f6.toString();
			long returnVal = -1;

			try {
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
				Date strDate = df.parse(strTime);
				returnVal = strDate.getTime();
			} catch (Exception e) {
				e.printStackTrace();
			}
			return returnVal;
		}
	};

	/*
	 * map function for each table FlatMap DataStream → DataStream it takes one
	 * element and produces zero, one, or more elements. A flatmap function that
	 * splits sentences to words:
	 */

	private static final FlatMapFunction<ObjectNode, Tuple7<String, String, String, String, Float, Float, Timestamp>> gpsFlatMapFunc = new FlatMapFunction<ObjectNode, Tuple7<String, String, String, String, Float, Float, Timestamp>>() {

		@Override
		public void flatMap(ObjectNode node,
				Collector<Tuple7<String, String, String, String, Float, Float, Timestamp>> collector) throws Exception {
			try {
				String sensorType = node.get("sensorType").asText();

				if (sensorType.equals("TYPE_GPS")) {
					String userId = node.get("userAccount").get("userId").asText();
					String userEmail = node.get("userAccount").get("userEmail").asText();
					String deviceId = node.get("deviceId").asText();
					Float lat = node.get("sensorValue").get("attr_lat").floatValue();
					Float lng = node.get("sensorValue").get("attr_lng").floatValue();
					Timestamp timeTag = new Timestamp(node.get("observationTimestamp").asLong());
					collector.collect(new Tuple7<>(userId, userEmail, deviceId, sensorType, lat, lng, timeTag));
					String tuple = userId + "," + userEmail + "," + deviceId + "," + sensorType + "," + lat + "," + lng
							+ "," + timeTag;
					// System.out.println(tuple);
					System.out.println("TYPE_GPS Sensor--> " + tuple);
					// store_into_MongoDB.GPSStore_MongoDB(new Tuple7<>(userId,userEmail,deviceId,
					// sensorType, lat, lng, timeTag));

				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	};

	/*
	 * private static class GropuByKeySelector implements KeySelector<Tuple8<String,
	 * String,String, String, Float, Float, Integer, Timestamp>, String> {
	 * 
	 * @Override public String getKey(Tuple8<String, String,String, String, Float,
	 * Float, Integer, Timestamp> value) { logger.info("GPS key = " + value.f0);
	 * return value.f0.trim(); } }
	 */
	private static class KafkaGPSStreamRegion implements SerializationSchema<Tuple7<String, String, String, String, Float, Float, Timestamp>>{
		private static ObjectMapper mapper = new ObjectMapper();

		@Override
		public byte[] serialize( Tuple7<String, String, String, String, Float, Float, Timestamp>  tuple) {
			byte[] res = null;
			try {
				ObjectNode obj = mapper.createObjectNode();

				JsonNode userId = mapper.valueToTree(tuple.f0);
				JsonNode userEmail = mapper.valueToTree(tuple.f1);
				JsonNode deviceId = mapper.valueToTree(tuple.f2);
				JsonNode sensorTypes = mapper.valueToTree(tuple.f3+"_withRegion");
				JsonNode GPSValue_attr_lat = mapper.valueToTree(tuple.f4);
				JsonNode GPSValue_attr_lng = mapper.valueToTree(tuple.f5);
				//JsonNode SensorValue = mapper.valueToTree(tuple.f6);

				java.util.Date today = new java.util.Date();

				java.sql.Timestamp ts1 = new java.sql.Timestamp(today.getTime());
				// ts1 = java.sql.Timestamp.valueOf("2005-04-06 09:01:10");

				long tsTime1 = ts1.getTime();

				JsonNode observationTimestamp = mapper.valueToTree(tsTime1);

				// this sub node for sensor value node
				ObjectNode objUserAccount = mapper.createObjectNode();
				objUserAccount.set("userId", userId);
				objUserAccount.set("userEmail", userEmail);


				// this sub node for sensor values
				// this to get region name
				String lan=GPSValue_attr_lat.toString();
				String lon=GPSValue_attr_lng.toString();

				String regoin=Get_Region_Name.Post_JSON(lan,lon);

				//ObjectMapper mapper = new ObjectMapper();
				JsonNode regoinNode = mapper.valueToTree(regoin);

				ObjectNode objvalue = mapper.createObjectNode();
				//objvalue.set("sensorValue", SensorValue);
				objvalue.set("attr_lat", GPSValue_attr_lat);
				objvalue.set("attr_lng", GPSValue_attr_lng);
				objvalue.set("regoin", regoinNode);

				// to extract sensor name e.eg TYPE_PROXIMITY will be PROXIMITY
				String SensorNames[]=tuple.f3.split("&", 2);
				String SensorName=SensorNames[0];


				obj.set("userAccount", objUserAccount); 
				obj.set("deviceId", deviceId);
				obj.set("sensorType",sensorTypes);
				obj.set("sensorValue", objvalue);
				//obj.set("GPSValue_attr_lat", GPSValue_attr_lat);
				//obj.set("GPSValue_attr_lng", GPSValue_attr_lng);
				//obj.set(SensorName+"_Value", SensorValue);
				obj.set("observationTimestamp", observationTimestamp);

				res = mapper.writeValueAsBytes(obj);

				System.out.println("Push "+SensorName+" Stream tuple to kafka topic desTopic(joinedstreams):\n" + obj);
			} catch (Exception e) {
				e.printStackTrace();
			}
			return res;
		}
	}

	@SuppressWarnings("static-access")

	public static void main(String[] args) throws Exception {
		// parse user parameters

		final ParameterTool parameterTool = ParameterTool.fromArgs(args);

		//String kafkaproperties = "/path/to/kafka.properties";
		//ParameterTool parameter = ParameterTool.fromPropertiesFile(kafkaproperties);

		String desTopic = parameterTool.getRequired("desTopic");
		int WindowSize = Integer.parseInt(parameterTool.getRequired("WindowSize"));
        String WindowType=parameterTool.getRequired("WindowType");
        String JobName=parameterTool.getRequired("JobName");

       // Configuration config = new Configuration();
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        
		// create execution environment
     	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// Kafak Consumer
		FlinkKafkaConsumer09 FlinkKafkaConsumer=new FlinkKafkaConsumer09<>(parameterTool.getRequired("topic"),
				new JSONDeserializationSchema(), parameterTool.getProperties());
		
		
		FlinkKafkaConsumer.setStartFromLatest();
		
		 
		//DataStream stream = env.addSource(new FlinkKafkaConsumer09<>(parameterTool.getRequired("topic"),
			//	new JSONDeserializationSchema(), parameterTool.getProperties()));

		
		DataStream stream= env.addSource(FlinkKafkaConsumer);
		// GPS Stream
		final DataStream<Tuple7<String, String, String, String, Float, Float, Timestamp>> GPSStream = stream
				.flatMap(gpsFlatMapFunc).assignTimestampsAndWatermarks(gpsTimeExtractor);

		//DataStream<Tuple7<String, String, String, String, Float, Float, Timestamp>> GPSStream2=KafkaGPSStreamRegion(GPSStream);
		/*
		 * To define new single value sensor 
		 * SensorsSingleValueType.add("sensor name");
		 */
		ArrayList<String> SensorsSingleValueType = new ArrayList<String>();
		// SensorsSingleValueType.add("TYPE_PROXIMITY");
		// SensorsSingleValueType.add("TYPE_LIGHT");
		// SensorsSingleValueType.add("TYPE_MICROPHONE");
		 SensorsSingleValueType.add("TYPE_WEATHER");
		 		// SensorsSingleValueType.add("TYPE_STEP_COUNTER");

		 
		/*
		 * To define new multiple values sensor
		 * SensorsMultipleValuesType.add("sensor name");
		 */
		ArrayList<String> SensorsMultipleValuesType = new ArrayList<String>();
		//SensorsMultipleValuesType.add("TYPE_ACCELEROMETER");
		// SensorsMultipleValuesType.add("TYPE_GRAVITY");

		int NumberofSensors = SensorsSingleValueType.size() + SensorsMultipleValuesType.size();

		System.out.println("Lis of Defined Sensors: " + NumberofSensors + " Sensors");

		// SingleValuesSensors
		System.out.println("-----------------------------------------------");
		System.out.println("Single Value Sensors");

		SingleValuesSensor_Stream_Template[] SingleValueSensorStreams = new SingleValuesSensor_Stream_Template[SensorsSingleValueType
		                                                                                                       .size()];

		for (int i = 0; i < SingleValueSensorStreams.length; i++)

		{
			SingleValueSensorStreams[i] = new SingleValuesSensor_Stream_Template();
			SingleValueSensorStreams[i].RequiredSensorType = SensorsSingleValueType.get(i);
			SingleValueSensorStreams[i].RequiredWindowType=WindowType;
			SingleValueSensorStreams[i].RequiredWindowSize=Integer.toString(WindowSize);
			SingleValueSensorStreams[i].RequiredJobName=JobName;

			
			
			
			
			if (WindowType.equals("Tumble"))
			 
			SingleValueSensorStreams[i].run_Tumbling_window(stream, GPSStream, parameterTool, desTopic, WindowSize);
			 
			 
			if (WindowType.equals("Sliding"))
				SingleValueSensorStreams[i].run_Sliding_window(stream, GPSStream, parameterTool, desTopic, WindowSize);
			 
			 
			if (WindowType.equals("Session"))
				SingleValueSensorStreams[i].run_Session_window(stream, GPSStream, parameterTool, desTopic, WindowSize);
 
		
		}

		System.out.println("-----------------------------------------------");

		System.out.println("Multiple Values Sensors");

		MultipleValuesSensor_Stream_Template[] MultipleValuesStreams = new MultipleValuesSensor_Stream_Template[SensorsMultipleValuesType
		                                                                                                        .size()];

		for (int i = 0; i < MultipleValuesStreams.length; i++) {
			MultipleValuesStreams[i] = new MultipleValuesSensor_Stream_Template();
			MultipleValuesStreams[i].RequriedSensorType = (SensorsMultipleValuesType.get(i)).trim();
			MultipleValuesStreams[i].run(stream, GPSStream, parameterTool, desTopic, WindowSize);
		}

		System.out.println("-----------------------------------------------");

		/*
		//Kafka GPS Producer 


		Timer timer = new Timer(); 

		timer.scheduleAtFixedRate(new TimerTask() {
			  @Override
			  public void run() {
			    // do staff
				  System.out.println("Hello world!");
				  final FlinkKafkaProducer09 kafkaProducer2 = new FlinkKafkaProducer09(parameterTool.getRequired("bootstrap.servers"),
							"android-sensor",
							new KafkaGPSStreamRegion());
				 DataStreamSink<Tuple7<String, String, String, String, Float, Float, Timestamp>> GPSRegoinStream;		
                  GPSStream.addSink(kafkaProducer2);			
				  }
			}, 0, 1000);

		 */

		env.execute(JobName);
		FileHandler fh = new FileHandler("streamsql.log");
		logger.addHandler(fh);


		
		System.exit(-1);




	}



}