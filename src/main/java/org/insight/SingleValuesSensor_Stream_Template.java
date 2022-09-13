package org.insight;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.api.java.tuple.Tuple13;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
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
 * join two streams (single sensor data with GPS ) 
 * 
 */
public class  SingleValuesSensor_Stream_Template implements java.io.Serializable  {
	private static final Logger logger = Logger.getGlobal();
	//public transient String RequriedSensorType;

	public  String RequiredSensorType;
	public static String RequiredWindowType;
	public static String RequiredWindowSize;
	public static String RequiredJobName;


	//private static final SingleValuesSensor_Stream_Template INSTANCE = new SingleValuesSensor_Stream_Template();




	private final static AscendingTimestampExtractor SingleValueSensortTimeExtractor = new AscendingTimestampExtractor<Tuple11<String, String,String, String, Integer,Integer,Integer,Integer,Integer, Timestamp,Timestamp>>() {
		@Override
		public long extractAscendingTimestamp(Tuple11<String, String,String, String, Integer,Integer,Integer,Integer,Integer, Timestamp,Timestamp> node) {
			String strTime = node.f10.toString();
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
	 *  map function for each table 
	 * FlatMap DataStream → DataStream  it takes one element and produces zero, 
		one, or more elements. A flatmap function that splits sentences to words:
	 */





	private  final FlatMapFunction<ObjectNode, Tuple11<String,String, String, String, Integer, Integer,Integer,Integer,Integer,Timestamp, Timestamp>> SingleValueSensorFlatMapFunc
	= new FlatMapFunction<ObjectNode, Tuple11<String,String, String, String, Integer,Integer,Integer,Integer,Integer, Timestamp,Timestamp>>() {



		@Override
		public void flatMap(ObjectNode node, Collector<Tuple11<String,String,String, String, Integer,Integer,Integer,Integer,Integer, Timestamp,Timestamp>> collector) throws Exception {
			try {

				System.out.println(node.toString()); 


				// to check sensor value size

				String NodesensorType = node.get("sensorType").asText();

				int sensorValueSize=node.get("sensorValue").size(); 



				// System.out.println("The consumed sensor data is "+NodesensorType+ " the checked sensor data is "+RequriedSensorType);  


				if (sensorValueSize>1&& NodesensorType.equals(RequiredSensorType))
				{


					String userId= node.get( "userAccount").get("userId").asText();
					String userEmail= node.get( "userAccount").get("userEmail").asText();
					String deviceId = node.get("deviceId").asText();
					Integer temp = node.get("sensorValue").get("temp").intValue();
					Integer humd = node.get("sensorValue").get("humd").intValue();
					Integer CO2 = node.get("sensorValue").get("CO2").intValue();
					Integer CO = node.get("sensorValue").get("CO").intValue();
					Integer hour=node.get("sensorValue").get("hour").intValue();

					//Time Created and Received 
					java.util.Date today = new java.util.Date();

					Timestamp observationTimestamp = new Timestamp(node.get("observationTimestamp").asLong());
					java.sql.Timestamp IngestTime = new java.sql.Timestamp(today.getTime());



					collector.collect(new Tuple11<>(userId,userEmail,deviceId, NodesensorType, temp,humd,CO2,CO,hour, observationTimestamp,IngestTime));
					String SensorValue=temp+","+humd+","+CO2+","+CO;
					String tuple=userId+","+userEmail+","+deviceId+","+NodesensorType+","+SensorValue+","+observationTimestamp+","+IngestTime;
					System.out.println(NodesensorType+ " Sensor--> "+tuple);
					//store_into_MongoDB.ProximityStore_MongoDB(new Tuple4<>(deviceId, sensorType, sensorValue, timeTag));
					//store_into_MongoDB.ProximityStore_MongoDB_2(new Tuple6<>(userId,userEmail,deviceId, sensorType, sensorValue, timeTag));

				}

			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	};


	public static final JoinFunction<Tuple11<String, String,String, String, Integer,Integer,Integer,Integer, Integer, Timestamp,Timestamp>,
	Tuple7<String, String,String, String, Float, Float, Timestamp>,
	Tuple13<String, String,String, String, Float, Float, Integer,Integer,Integer,Integer,Integer, Timestamp,Timestamp>> joinStreamFuncSensorGPS
	= new JoinFunction<Tuple11<String, String,String, String, Integer,Integer,Integer,Integer,Integer, Timestamp,Timestamp>,
	Tuple7<String, String, String, String, Float, Float, Timestamp>,
	Tuple13<String, String,String, String, Float, Float, Integer,Integer,Integer,Integer,Integer, Timestamp,Timestamp>>() {
		@Override
		public Tuple13<String, String,String, String, Float, Float, Integer,Integer,Integer,Integer,Integer, Timestamp,Timestamp> join(Tuple11<String, String,String, String, Integer,Integer,Integer,Integer,Integer, Timestamp,Timestamp> SensorStream,
				Tuple7<String, String,String, String, Float, Float, Timestamp> GPSStream) {
			//logger.info("In Join "+RequriedSensorType+" GPS Streams Function");

			/*return new Tuple6<String, String, Float, Float, Integer, Timestamp>("Joined id " + gps.f0,
			gps.f1,
			gps.f2,
			gps.f3,
			Proximity.f2,
			gps.f4);
			 */


			Tuple13<String, String, String, String, Float, Float, Integer,Integer,Integer,Integer, Integer,Timestamp,Timestamp> joinedtuple=
					new Tuple13<String, String,String, String, Float, Float, Integer,Integer,Integer,Integer, Integer,Timestamp,Timestamp>( 
							GPSStream.f0, // userId
							GPSStream.f1, // userEmail
							GPSStream.f2, // deviceId
							("TYPE_Join_"+SensorStream.f3.substring(5)+"_"+GPSStream.f3.substring(5)), // sensorsType
							GPSStream.f4, // sensor-value-attr_lat
							GPSStream.f5, // sensor-value-attr_lng
							SensorStream.f4, // temp
							SensorStream.f5, // humd
							SensorStream.f6, // CO2
							SensorStream.f7, // CO
							SensorStream.f8, // hour
							//Time Stamp
							SensorStream.f9,//SensorStream observationTimestamp: Created
							SensorStream.f10//SensorStream observationTimestamp: IngestTime
							//SensorStream.f6 ////SensorStream observationTimestamp: Processed
							);  
			//store_into_MongoDB.Joined_ProximityGPSStream_Store_MongoDB(joinedtuple);
			return joinedtuple;
		}

	};

	private static class SingleValueSensorKeySelector implements KeySelector<Tuple11<String, String,String, String,Integer, Integer,Integer,Integer, Integer, Timestamp,Timestamp>, String> {
		@Override
		public String getKey(Tuple11<String, String,String, String, Integer,Integer,Integer,Integer,  Integer,Timestamp,Timestamp> value) {
			logger.info("Sensor"+  value.f2+ "key = " + value.f0);
			return value.f0.trim();


		}
	}


	private static class GPSKeySelector implements KeySelector<Tuple7<String, String,String, String, Float, Float, Timestamp>, String> {
		@Override
		public String getKey(Tuple7<String, String, String, String, Float, Float, Timestamp> value) {
			logger.info("GPS key = " + value.f0);
			return value.f0;
		}
	}




	private static class KafkaJoinedStreamSchema implements SerializationSchema<Tuple13<String, String, String, String, Float, Float, Integer,Integer,Integer,Integer, Integer, Timestamp,Timestamp>>{
		private static ObjectMapper mapper = new ObjectMapper();

		@Override
		public byte[] serialize(Tuple13<String, String, String, String, Float, Float, Integer,Integer,Integer,Integer, Integer, Timestamp,Timestamp> tuple) {
			byte[] res = null;
			try {
				ObjectNode obj = mapper.createObjectNode();


				JsonNode userId = mapper.valueToTree(tuple.f0);
				JsonNode userEmail = mapper.valueToTree(tuple.f1);
				JsonNode deviceId = mapper.valueToTree(tuple.f2);
				JsonNode sensorTypes = mapper.valueToTree(tuple.f3);
				//sensor values
				JsonNode GPSValue_attr_lat = mapper.valueToTree(tuple.f4);
				JsonNode GPSValue_attr_lng = mapper.valueToTree(tuple.f5);
				JsonNode TempValue = mapper.valueToTree(tuple.f6);
				JsonNode HumidValue = mapper.valueToTree(tuple.f7);
				JsonNode CO2Value = mapper.valueToTree(tuple.f8);
				JsonNode COValue = mapper.valueToTree(tuple.f9);
				JsonNode hourValue = mapper.valueToTree(tuple.f10);
				//time
				JsonNode observationTimestamp = mapper.valueToTree(tuple.f11);
				JsonNode IngestTime = mapper.valueToTree(tuple.f12);
				//JsonNode observationTimestamp = mapper.valueToTree(tuple.f7);


				// this sub node for Window configuration 
				ObjectNode objWindowConfig = mapper.createObjectNode();
				JsonNode JobName = mapper.valueToTree(RequiredJobName);
				JsonNode WindowType = mapper.valueToTree(RequiredWindowType);
				JsonNode WindowSize = mapper.valueToTree(RequiredWindowSize);
				objWindowConfig.set("JobName", JobName);
				objWindowConfig.set("WindowType", WindowType);
				objWindowConfig.set("WindowSize", WindowSize);


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

				// Apply weather variables conditions for alert color
				int Temperature=tuple.f6;
				int Humidity=tuple.f7;
				int CO2=tuple.f8;
				int CO=tuple.f9;



				/*String FireAlertColor="";

				if((Temperature<30) && (Humidity>30) && 
						(CO2<350) && (CO<10))
						FireAlertColor="Green";

				if((Temperature>=30 && Temperature<37) && (Humidity<=30 && Humidity>20) && 
					(CO2>=350 && CO2<2000) && (CO>=10 && CO<25))
					FireAlertColor="Yellow";


				else if((Temperature>=37 && Temperature<40) && (Humidity<=20 && Humidity>10) && 
						(CO2>=2000 && CO2<5000) && (CO>=25 && CO<50))
						FireAlertColor="Orange";

				else if((Temperature>=40) && (Humidity<=10) && 
						(CO2>=5000) && (CO>=50))
						FireAlertColor="Red";

				//else
					//FireAlertColor="FlasePostive"; 
				 */

				String FireAlertColor="";

				if((Temperature<30) && (Humidity>30) && 
						(CO2<350) && (CO<10))
					FireAlertColor="Green";

				if((Temperature>=30 && Temperature<37) || 
						(Humidity<=30 && Humidity>20)  ||
						(CO2>=350 && CO2<2000) || (CO>=10 && CO<25))
					FireAlertColor="Yellow";


				else if((Temperature>=37 && Temperature<40) || 
						(Humidity<=20 && Humidity>10)       ||
						(CO2>=2000 && CO2<5000)             || 
						(CO>=25 && CO<50))
			   FireAlertColor="Orange";

				else if((Temperature>=40) && (Humidity<=10) || 
						(CO2>=5000) && (CO>=50))
			    FireAlertColor="Red";

				//else
				//FireAlertColor="FlasePostive"; 




				JsonNode FireAlert=mapper.valueToTree(FireAlertColor);

				ObjectNode objvalue = mapper.createObjectNode();
				objvalue.set("attr_lat", GPSValue_attr_lat);
				objvalue.set("attr_lng", GPSValue_attr_lng);
				objvalue.set("Hour", hourValue);
				objvalue.set("Temperature", TempValue);
				objvalue.set("Humidity", HumidValue);
				objvalue.set("CO2", CO2Value);
				objvalue.set("CO", COValue);
				objvalue.set("regoin", regoinNode);
				objvalue.set("FireAlert", FireAlert);



				// to extract sensor name e.eg TYPE_PROXIMITY will be PROXIMITY
				String SensorNames[]=tuple.f3.split("&", 2);
				String SensorName=SensorNames[0];


				// this sub node for sensor value node
				ObjectNode objTime = mapper.createObjectNode();

				java.util.Date today = new java.util.Date();
				java.sql.Timestamp processedTime = new java.sql.Timestamp(today.getTime());
				JsonNode processedTimestamp = mapper.valueToTree(processedTime);

				objTime.set("ObservationTimestamp", observationTimestamp);
				objTime.set("IngestTime", IngestTime);
				objTime.set("ProcessedTimestamp", processedTimestamp);


				obj.set("WindowConfig",objWindowConfig); 
				obj.set("userAccount", objUserAccount); 
				obj.set("deviceId", deviceId);
				obj.set("sensorType",sensorTypes);
				obj.set("sensorValue", objvalue);
				//obj.set("GPSValue_attr_lat", GPSValue_attr_lat);
				//obj.set("GPSValue_attr_lng", GPSValue_attr_lng);
				//obj.set(SensorName+"_Value", SensorValue);
				obj.set("Time", objTime);

				res = mapper.writeValueAsBytes(obj);

				System.out.println("Push "+SensorName+" Stream tuple to kafka topic desTopic(joinedstreams):\n" + obj);
			} catch (Exception e) {
				e.printStackTrace();
			}
			return res;
		}
	}






	public void run_Tumbling_window (DataStream stream, DataStream<Tuple7<String, String,String, String, Float, Float, Timestamp>> GPSStream, ParameterTool parameterTool, String desTopic,int WindowSize )
	{


		// this.RequriedSensorType=SensorType;
		System.out.println("Strating Consume Sensor Stream "+  RequiredSensorType );


		DataStream<Tuple11<String, String,String, String, Integer,Integer,Integer,Integer,Integer, Timestamp,Timestamp>> SensorStream = stream.flatMap(SingleValueSensorFlatMapFunc)
				.assignTimestampsAndWatermarks(SingleValueSensortTimeExtractor);


		//System.out.println("Starting Consume Sensor Stream "+  RequriedSensorType );


		FlinkKafkaProducer09 kafkaProducer = new FlinkKafkaProducer09(parameterTool.getRequired("bootstrap.servers"),
				desTopic,
				new KafkaJoinedStreamSchema());

		DataStreamSink<Tuple13<String, String,String, String, Float, Float, Integer,Integer,Integer,Integer,Integer, Timestamp,Timestamp>> outputJoinedSensorGPSdStreams=		

				SensorStream.join(GPSStream)
				.where(new SingleValueSensorKeySelector())
				.equalTo(new GPSKeySelector())
				// .window(TumblingEventTimeWindows.of(Time.seconds(10000)))
				// .window(SlidingEventTimeWindows.of(Time.minutes(5) /* size */
				//  , Time.seconds(150) /* slide */))
				//.window(EventTimeSessionWindows.withGap(Time.minutes(5)))
				.window(TumblingEventTimeWindows.of(Time.seconds(WindowSize)))
				.apply(joinStreamFuncSensorGPS).addSink(kafkaProducer);
	}




	public void run_Sliding_window (DataStream stream, DataStream<Tuple7<String, String,String, String, Float, Float, Timestamp>> GPSStream, ParameterTool parameterTool, String desTopic,int WindowSize )
	{


		// this.RequriedSensorType=SensorType;
		System.out.println("Strating Consume Sensor Stream "+  RequiredSensorType );


		DataStream<Tuple11<String, String,String, String, Integer,Integer,Integer,Integer,Integer, Timestamp,Timestamp>> SensorStream = stream.flatMap(SingleValueSensorFlatMapFunc)
				.assignTimestampsAndWatermarks(SingleValueSensortTimeExtractor);


		//System.out.println("Strating Consume Sensor Stream "+  RequriedSensorType );


		FlinkKafkaProducer09 kafkaProducer = new FlinkKafkaProducer09(parameterTool.getRequired("bootstrap.servers"),
				desTopic,
				new KafkaJoinedStreamSchema());

		DataStreamSink<Tuple13<String, String,String, String, Float, Float, Integer,Integer,Integer,Integer,Integer, Timestamp,Timestamp>> outputJoinedSensorGPSdStreams=		

				SensorStream.join(GPSStream)
				.where(new SingleValueSensorKeySelector())
				.equalTo(new GPSKeySelector())
				// .window(TumblingEventTimeWindows.of(Time.seconds(10000)))
				.window(SlidingEventTimeWindows.of(Time.seconds(WindowSize) /* size */
						, Time.seconds(WindowSize/2) /* slide */))
				//.window(EventTimeSessionWindows.withGap(Time.minutes(5)))
				//.window(TumblingEventTimeWindows.of(Time.minutes(WindowSize)))
				.apply(joinStreamFuncSensorGPS).addSink(kafkaProducer);
	}

	public void run_Session_window (DataStream stream, DataStream<Tuple7<String, String,String, String, Float, Float, Timestamp>> GPSStream, ParameterTool parameterTool, String desTopic,int WindowSize )
	{


		// this.RequriedSensorType=SensorType;
		System.out.println("Strating Consume Sensor Stream "+  RequiredSensorType );


		DataStream<Tuple11<String, String,String, String, Integer,Integer,Integer,Integer,Integer, Timestamp,Timestamp>> SensorStream = stream.flatMap(SingleValueSensorFlatMapFunc)
				.assignTimestampsAndWatermarks(SingleValueSensortTimeExtractor);


		//System.out.println("Strating Consume Sensor Stream "+  RequriedSensorType );


		FlinkKafkaProducer09 kafkaProducer = new FlinkKafkaProducer09(parameterTool.getRequired("bootstrap.servers"),
				desTopic,
				new KafkaJoinedStreamSchema());

		DataStreamSink<Tuple13<String, String,String, String, Float, Float,Integer, Integer,Integer,Integer,Integer, Timestamp,Timestamp>> outputJoinedSensorGPSdStreams=		

				SensorStream.join(GPSStream)
				.where(new SingleValueSensorKeySelector())
				.equalTo(new GPSKeySelector())
				// .window(TumblingEventTimeWindows.of(Time.seconds(10000)))
				// .window(SlidingEventTimeWindows.of(Time.minutes(5) /* size */
				//  , Time.seconds(150) /* slide */))
				.window(EventTimeSessionWindows.withGap(Time.seconds(WindowSize)))
				//.window(TumblingEventTimeWindows.of(Time.minutes(WindowSize)))
				.apply(joinStreamFuncSensorGPS).addSink(kafkaProducer);
	}

}