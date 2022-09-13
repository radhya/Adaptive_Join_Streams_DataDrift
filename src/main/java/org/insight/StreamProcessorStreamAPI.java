package org.insight;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.ParameterTool;
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
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
//import org.insight.BeijingTrajectoryFL1_4.UserLocationSchema;

 
/* @ Radhya Sahal
 * This program to subsrcibe data from Kafka and then
 * join two streams (sensor data using proximity and GPS ) 
 * 
 * using these agrgumments 
 * --topic android-sensor --bootstrap.servers 140.203.155.187:8017,140.203.155.187:8018,140.203.155.187:8019 
 * --zookeeper.connect 140.203.155.187:8020 --group.id testgroupid
 * Note: the mobile app should registered with use account
 * in case of not, Flink can't map tuples with null account which causes  NullPointerException 
 * 
 */
public class StreamProcessorStreamAPI {
	private static final Logger logger = Logger.getGlobal();
	 	private final static AscendingTimestampExtractor ProximityTimeExtractor = new AscendingTimestampExtractor<Tuple4<String, String, Integer, Timestamp>>() {
		@Override
		public long extractAscendingTimestamp(Tuple4<String, String, Integer, Timestamp> node) {
			String strTime = node.f3.toString();
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

	private final static AscendingTimestampExtractor gpsTimeExtractor = new AscendingTimestampExtractor<Tuple5<String, String, Float, Float, Timestamp>>() {
		@Override
		public long extractAscendingTimestamp(Tuple5<String, String, Float, Float, Timestamp> node) {
			String strTime = node.f4.toString();
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
    
	private static final MapFunction<ObjectNode, Tuple4<String, String, Integer, Timestamp>> ProximityMapFunction
			= new MapFunction<ObjectNode, Tuple4<String, String, Integer, Timestamp>>() {

		@Override
		public Tuple4<String, String, Integer, Timestamp> map(ObjectNode node) throws Exception {

			Tuple4<String, String, Integer, Timestamp> resp = null;

			String sensorType = node.get("sensorType").asText();

			if (sensorType.equals("TYPE_PROXIMITY")) {
				String deviceId = node.get("deviceId").asText();
				Integer sensorValue = node.get("sensorValue").intValue();
				Timestamp timeTag = new Timestamp(node.get("observationTimestamp").asLong());
				resp = new Tuple4<>(deviceId, sensorType, sensorValue, timeTag);
			
			}
			
			return resp;
		}
	};

	
	
	private static final FlatMapFunction<ObjectNode, Tuple4<String, String, Integer, Timestamp>> ProximityFlatMapFunc
			= new FlatMapFunction<ObjectNode, Tuple4<String, String, Integer, Timestamp>>() {

		@Override
		public void flatMap(ObjectNode node, Collector<Tuple4<String, String, Integer, Timestamp>> collector) throws Exception {
			try {
				String sensorType = node.get("sensorType").asText();

				if (sensorType.equals("TYPE_PROXIMITY")) {
					String deviceId = node.get("deviceId").asText();
					String userId= node.get( "userAccount").get("userId").asText();
					String userEmail= node.get( "userAccount").get("userEmail").asText();
					Integer sensorValue = node.get("sensorValue").intValue();
					Timestamp timeTag = new Timestamp(node.get("observationTimestamp").asLong());
					collector.collect(new Tuple4<>(deviceId, sensorType, sensorValue, timeTag));
				    String tuple=deviceId+","+sensorType+","+sensorValue+","+timeTag;
					System.out.println("TYPE_PROXIMITY--> "+tuple);
					//store_into_MongoDB.ProximityStore_MongoDB(new Tuple4<>(deviceId, sensorType, sensorValue, timeTag));
					//store_into_MongoDB.ProximityStore_MongoDB_2(new Tuple6<>(userId,userEmail,deviceId, sensorType, sensorValue, timeTag));

				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	};

	private static final FlatMapFunction<ObjectNode, Tuple5<String, String, Float, Float, Timestamp>> gpsFlatMapFunc
			= new FlatMapFunction<ObjectNode, Tuple5<String, String, Float, Float, Timestamp>>() {

		@Override
		public void flatMap(ObjectNode node, Collector<Tuple5<String, String, Float, Float, Timestamp>> collector) throws Exception {
			try {
				String sensorType = node.get("sensorType").asText();
				if (sensorType.equals("TYPE_GPS")) {
					String deviceId = node.get("deviceId").asText();
					String userId= node.get( "userAccount").get("userId").asText();
					String userEmail= node.get( "userAccount").get("userEmail").asText();
					Float lat = node.get("sensorValue").get("attr_lat").floatValue();
					Float lng = node.get("sensorValue").get("attr_lng").floatValue();
					Timestamp timeTag = new Timestamp(node.get("observationTimestamp").asLong());
					collector.collect(new Tuple5<>(deviceId, sensorType, lat, lng, timeTag));
					String tuple=deviceId+","+sensorType+","+lat+","+lng+","+timeTag;
					System.out.println(tuple);
					System.out.println("TYPE_GPS--> "+ tuple);
					//store_into_MongoDB.GPSStore_MongoDB(new Tuple7<>(userId,userEmail,deviceId, sensorType, lat, lng, timeTag));

				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	};

	private static class ProximityKeySelector implements KeySelector<Tuple4<String, String, Integer, Timestamp>, String> {
		@Override
		public String getKey(Tuple4<String, String, Integer, Timestamp> value) {
			logger.info("Proximity key = " + value.f0);
			return value.f0.trim();
		}
	}

	
	private static class GropuByKeySelector implements KeySelector<Tuple6<String, String, Float, Float, Integer, Timestamp>, String> {
		@Override
		public String getKey(Tuple6<String, String, Float, Float, Integer, Timestamp> value) {
			logger.info("GPS key = " + value.f0);
			return value.f0.trim();
		}
	}
	
	
	private static class GPSKeySelector implements KeySelector<Tuple5<String, String, Float, Float, Timestamp>, String> {
		@Override
		public String getKey(Tuple5<String, String, Float, Float, Timestamp> value) {
			logger.info("GPS key = " + value.f0);
			return value.f0;
		}
	}

	private static final JoinFunction<Tuple4<String, String, Integer, Timestamp>,
			Tuple5<String, String, Float, Float, Timestamp>,
			Tuple6<String, String, Float, Float, Integer, Timestamp>> joinStreamFuncPROXGPS
			= new JoinFunction<Tuple4<String, String, Integer, Timestamp>,
			Tuple5<String, String, Float, Float, Timestamp>,
			Tuple6<String, String, Float, Float, Integer, Timestamp>>() {
		@Override
		public Tuple6<String, String, Float, Float, Integer, Timestamp> join(Tuple4<String, String, Integer, Timestamp> Proximity,
																		Tuple5<String, String, Float, Float, Timestamp> gps) {
			logger.info("In join 2 streams function");
			
			/*return new Tuple6<String, String, Float, Float, Integer, Timestamp>("Joined id " + gps.f0,
					gps.f1,
					gps.f2,
					gps.f3,
					Proximity.f2,
					gps.f4);
					*/
			
			
			Tuple6<String, String, Float, Float, Integer, Timestamp> joinedtuple=
					new Tuple6<String, String, Float, Float, Integer, Timestamp>( 
			        gps.f0, // deviceId
					gps.f1, // sensorType
					gps.f2, // sensorType-attr_lat
					gps.f3, // sensorType-attr_lng
					Proximity.f2, // ProximitysensorValue
					gps.f4); // GPS observationTimestamp
			//store_into_MongoDB.Joined_ProximityGPSStream_Store_MongoDB(joinedtuple);
			return joinedtuple;
		}
		
	};


	public static void main(String[] args) throws Exception {
		// parse user parameters
		ParameterTool parameterTool = ParameterTool.fromArgs(args);

		//create execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// read data from Kafak 
		DataStream stream = env.addSource(
				new FlinkKafkaConsumer09<>(parameterTool.getRequired("topic")
						, new JSONDeserializationSchema(), parameterTool.getProperties()));

		/* Resource discovery module to discover sensors
		 *  Q1: How many sensors ? 
		 *  Q2: Are they have the same data format  
		 */
		
		/*
		 * for storing into MongoDB
		 */
		
		
		DataStream<Tuple4<String, String, Integer, Timestamp>> ProximityStream = stream.flatMap(ProximityFlatMapFunc)
																			  .assignTimestampsAndWatermarks(ProximityTimeExtractor);

		DataStream<Tuple5<String, String, Float, Float, Timestamp>> gpsStream = stream.flatMap(gpsFlatMapFunc)
		                                                                          .assignTimestampsAndWatermarks(gpsTimeExtractor);
	DataStreamSink<Tuple6<String, String, Float, Float, Integer, Timestamp>> outputstream=		
		
			ProximityStream.join(gpsStream)
			.where(new ProximityKeySelector())
			.equalTo(new GPSKeySelector())
			.window(TumblingEventTimeWindows.of(Time.milliseconds(10000)))
			.apply(joinStreamFuncPROXGPS).print();
			 
		//	.writeAsCsv("output.csv", WriteMode.OVERWRITE, "\n", "|").setParallelism(1);//.print();//
	System.out.println(outputstream.toString());
	
	/*
	FlinkKafkaProducer09 kafkaProducer = new FlinkKafkaProducer09(parameterTool.getRequired("bootstrap.servers"),
			outputstream,
			new UserLocationSchema());

	gpsStream.addSink(kafkaProducer);
	//keyBy(0).timeWindow(Time.seconds(1));
			
	/*ProximityStream.join(gpsStream)
				.where(new ProximityKeySelector())
				.equalTo(new GPSKeySelector())
				.window(TumblingEventTimeWindows.of(Time.seconds(5)))
				.apply(joinStreamFunc).writeAsCsv("output.csv", WriteMode.OVERWRITE, "\n", "|").setParallelism(1);//.print();//
		//keyBy(0).timeWindow(Time.seconds(1));
				//.join(ProximityStream)
				//.where(new GropuByKeySelector())
				//.equals(new GPSKeySelector());
			
		*/
		//outputstream.keyBy(new GropuByKeySelector()).window(<Tuple6<String, String, Float, Float, Integer, Timestamp>)
		env.execute();
		FileHandler fh = new FileHandler("streamsql.log");   
		logger.addHandler(fh); 
	}
}