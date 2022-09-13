package org.insight;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple13;
import org.apache.flink.api.java.tuple.Tuple14;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
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
public class MultipleValuesSensor_Stream_Template implements java.io.Serializable {
	private static final Logger logger = Logger.getGlobal();
	// public transient String RequriedSensorType;

	public String RequriedSensorType;

	// private static final SingleValuesSensor_Stream_Template INSTANCE = new
	// SingleValuesSensor_Stream_Template();

	private final static AscendingTimestampExtractor MultiValuesSensortTimeExtractor = new AscendingTimestampExtractor<Tuple11<String, String, String, String, String, Float, String, Float, String, Float, Timestamp>>() {
		@Override
		public long extractAscendingTimestamp(
				Tuple11<String, String, String, String, String, Float, String, Float, String, Float, Timestamp> node) {
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
	 * map function for each table FlatMap DataStream → DataStream it takes one
	 * element and produces zero, one, or more elements. A flatmap function that
	 * splits sentences to words:
	 */

	private final FlatMapFunction<ObjectNode, Tuple11<String, String, String, String, String, Float, String, Float, String, Float, Timestamp>> MultiValueSensorFlatMapFunc = new FlatMapFunction<ObjectNode, Tuple11<String, String, String, String, String, Float, String, Float, String, Float, Timestamp>>() {

		@Override
		public void flatMap(ObjectNode node,
				Collector<Tuple11<String, String, String, String, String, Float, String, Float, String, Float, Timestamp>> collector)
				throws Exception {
			try {

				System.out.println(node.toString());

				// to check sensor value size
				String NodesensorType = node.get("sensorType").asText();

				int sensorValueSize = node.get("sensorValue").size();

				// System.out.println("The consumed sensor data is "+NodesensorType+ " the
				// checked sensor data is "+RequriedSensorType);

				if (sensorValueSize > 0 && NodesensorType.equals(RequriedSensorType)) {

					// get multiple values

					Iterator<String> fieldsIterator = node.get("sensorValue").fieldNames();

					// HashMap<String, Float> SensorValues = new HashMap<String, Float>();

					String SensorValueName[] = new String[sensorValueSize];
					Float SensorValue[] = new Float[sensorValueSize];

					int SensorValuesCounter = 0;
					while (fieldsIterator.hasNext()) {
						String filedname = fieldsIterator.next();
						// System.out.println(filedname+"SensorValuesCounter"+ SensorValuesCounter);

						SensorValueName[SensorValuesCounter] = filedname;
						SensorValue[SensorValuesCounter] = node.get("sensorValue").get(filedname).floatValue();
						SensorValuesCounter++;
					} // end while

					String userId = node.get("userAccount").get("userId").asText();
					String userEmail = node.get("userAccount").get("userEmail").asText();
					String deviceId = node.get("deviceId").asText();
					// Integer sensorValue = node.get("sensorValue").intValue();

					Timestamp timeTag = new Timestamp(node.get("observationTimestamp").asLong());
					collector.collect(new Tuple11<>(userId, userEmail, deviceId, NodesensorType, SensorValueName[0],
							SensorValue[0], SensorValueName[1], SensorValue[1], SensorValueName[2], SensorValue[2],
							timeTag));

					String tuple = "{" + userId + "," + userEmail + "," + deviceId + "," + NodesensorType + ","
							+ "SensorValues{" + SensorValueName[0] + ":" + SensorValue[0] + "," + SensorValueName[1]
							+ ":" + SensorValue[1] + "," + SensorValueName[2] + ":" + SensorValue[2] + "," + timeTag
							+ "}";

					System.out.println(NodesensorType + " Sensor--> " + tuple);

				}

			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	};

	private static class MultipleValueSensorKeySelector implements
			KeySelector<Tuple11<String, String, String, String, String, Float, String, Float, String, Float, Timestamp>, String> {
		@Override
		public String getKey(
				Tuple11<String, String, String, String, String, Float, String, Float, String, Float, Timestamp> value) {
			logger.info("Sensor" + value.f2 + "key = " + value.f0);
			return value.f0.trim();

		}
	}

	private static class MultipleValuesSensorKeySelector implements
			KeySelector<Tuple11<String, String, String, String, String, Float, String, Float, String, Float, Timestamp>, String> {
		@Override
		public String getKey(
				Tuple11<String, String, String, String, String, Float, String, Float, String, Float, Timestamp> value) {
			logger.info("GPS key = " + value.f0);
			return value.f0;
		}
	}

	private static class GPSKeySelector
			implements KeySelector<Tuple7<String, String, String, String, Float, Float, Timestamp>, String> {
		@Override
		public String getKey(Tuple7<String, String, String, String, Float, Float, Timestamp> value) {
			logger.info("GPS key = " + value.f0);
			return value.f0;
		}
	}

	//
	public static final JoinFunction<Tuple11<String, String, String, String, String, Float, String, Float, String, Float, Timestamp>, Tuple7<String, String, String, String, Float, Float, Timestamp>, Tuple13<String, String, String, String, Float, Float, String, Float, String, Float, String, Float, Timestamp>> joinStreamFuncSensorGPS

			= new JoinFunction<Tuple11<String, String, String, String, String, Float, String, Float, String, Float, Timestamp>, Tuple7<String, String, String, String, Float, Float, Timestamp>, Tuple13<String, String, String, String, Float, Float, String, Float, String, Float, String, Float, Timestamp>>() {
				@Override
				public Tuple13<String, String, String, String, Float, Float, String, Float, String, Float, String, Float, Timestamp> join(
						Tuple11<String, String, String, String, String, Float, String, Float, String, Float, Timestamp> SensorStream,
						Tuple7<String, String, String, String, Float, Float, Timestamp> GPSStream) {
					// logger.info("In Join "+RequriedSensorType+" GPS Streams Function");

					Tuple13<String, String, String, String, Float, Float, String, Float, String, Float, String, Float, Timestamp> joinedtuple = new Tuple13<String, String, String, String, Float, Float, String, Float, String, Float, String, Float, Timestamp>(
							GPSStream.f0, // userId
							GPSStream.f1, // userEmail
							GPSStream.f2, // deviceId
							("TYPE_Join_" + SensorStream.f3.substring(5) + "_" + GPSStream.f3.substring(5)), // sensor
																												// Type
							GPSStream.f4, // sensor-value-attr_lat
							GPSStream.f5, // sensor-value-attr_lng
							SensorStream.f4, // sensorValueName[0]
							SensorStream.f5, // sensorValue[0]
							SensorStream.f6, // sensorValueName[1]
							SensorStream.f7, // sensorValue[1]
							SensorStream.f8, // sensorValueName[2]
							SensorStream.f9, // sensorValue[2]
							GPSStream.f6); // GPS observationTimestamp
					// store_into_MongoDB.Joined_ProximityGPSStream_Store_MongoDB(joinedtuple);
					return joinedtuple;
				}

			};

	private static class KafkaJoinedStreamSchema implements
			SerializationSchema<Tuple13<String, String, String, String, Float, Float, String, Float, String, Float, String, Float, Timestamp>> {
		private static ObjectMapper mapper = new ObjectMapper();

		@Override
		public byte[] serialize(
				Tuple13<String, String, String, String, Float, Float, String, Float, String, Float, String, Float, Timestamp> tuple) {
			byte[] res = null;
			try {
				ObjectNode obj = mapper.createObjectNode();

				JsonNode userId = mapper.valueToTree(tuple.f0);
				JsonNode userEmail = mapper.valueToTree(tuple.f1);
				JsonNode deviceId = mapper.valueToTree(tuple.f2);
				JsonNode sensorTypes = mapper.valueToTree(tuple.f3);
				// node values
				JsonNode GPSValue_attr_lat = mapper.valueToTree(tuple.f4);
				JsonNode GPSValue_attr_lng = mapper.valueToTree(tuple.f5);
				JsonNode SensorVlaueName1 = mapper.valueToTree(tuple.f6);
				JsonNode SensorVlaueValue1 = mapper.valueToTree(tuple.f7);
				JsonNode SensorVlaueName2 = mapper.valueToTree(tuple.f8);
				JsonNode SensorVlaueValue2 = mapper.valueToTree(tuple.f9);
				JsonNode SensorVlaueName3 = mapper.valueToTree(tuple.f10);
				JsonNode SensorVlaueValue3 = mapper.valueToTree(tuple.f11);

				JsonNode observationTimestamp = mapper.valueToTree(tuple.f12);

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
				objvalue.set(SensorVlaueName1.toString(), SensorVlaueValue1);
				objvalue.set(SensorVlaueName2.toString(), SensorVlaueValue2);
				objvalue.set(SensorVlaueName3.toString(), SensorVlaueValue3);

				objvalue.set("attr_lat", GPSValue_attr_lat);
				objvalue.set("attr_lng", GPSValue_attr_lng);
				objvalue.set("regoin", regoinNode);

				// to extract sensor name e.eg TYPE_PROXIMITY will be PROXIMITY
				String SensorNames[] = tuple.f3.split("&", 2);
				String SensorName = SensorNames[0];

				obj.set("userAccount", objUserAccount);
				obj.set("deviceId", deviceId);
				obj.set("sensorType", sensorTypes);
				obj.set("sensorValue", objvalue);

				obj.set("observationTimestamp", observationTimestamp);

				res = mapper.writeValueAsBytes(obj);

				System.out.println(
						"Push " + SensorName + " Stream tuple to kafka topic desTopic(joinedstreams):\n" + obj);
			} catch (Exception e) {
				e.printStackTrace();
			}
			return res;
		}
	}

	public void run(DataStream stream,
			DataStream<Tuple7<String, String, String, String, Float, Float, Timestamp>> GPSStream,
			ParameterTool parameterTool, String desTopic, int WindowSize) {

		// this.RequriedSensorType=SensorType;
		System.out.println("Strating Consume Sensor Stream " + RequriedSensorType);

		DataStream<Tuple11<String, String, String, String, String, Float, String, Float, String, Float, Timestamp>> MultipleValueSensorStream = stream
				.flatMap(MultiValueSensorFlatMapFunc).assignTimestampsAndWatermarks(MultiValuesSensortTimeExtractor);

		FlinkKafkaProducer09 kafkaProducer = new FlinkKafkaProducer09(parameterTool.getRequired("bootstrap.servers"),
				desTopic, new KafkaJoinedStreamSchema());

		DataStreamSink<Tuple13<String, String, String, String, Float, Float, String, Float, String, Float, String, Float, Timestamp>> outputJoinedSensorGPSdStreams =

				MultipleValueSensorStream.join(GPSStream).where(new MultipleValuesSensorKeySelector())
						.equalTo(new GPSKeySelector()).window(TumblingEventTimeWindows.of(Time.milliseconds(10000)))
						.apply(joinStreamFuncSensorGPS).addSink(kafkaProducer);

	}

}