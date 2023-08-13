package jarvey.assoc.global_track;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes.ByteArraySerde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Range;

import io.confluent.common.utils.TestUtils;
import jarvey.assoc.OverlapArea;
import jarvey.assoc.OverlapAreaRegistry;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TrackGlobalObjectDockerMain {
	private static final Logger s_logger = LoggerFactory.getLogger(TrackGlobalObjectDockerMain.class);
	
	public static void main(String... args) throws Exception {
		Map<String,String> envs = System.getenv();
		
		OverlapAreaRegistry areas = new OverlapAreaRegistry();
		OverlapArea etriTestbed = new OverlapArea("etri_testbed");
		etriTestbed.addOverlap("etri:04", "etri:05");
		etriTestbed.addOverlap("etri:04", "etri:06");
		etriTestbed.addOverlap("etri:05", "etri:06");
		etriTestbed.addOverlap("etri:05", "etri:07");
		areas.add("etri_testbed", etriTestbed);
		
		Map<String,Range<Double>> VALID_DISTANCE_THRESHOLD = Maps.newHashMap();
		VALID_DISTANCE_THRESHOLD.put("etri:04", Range.closed(37d, 50d));
		VALID_DISTANCE_THRESHOLD.put("etri:05", Range.closed(50d, 60d));
		VALID_DISTANCE_THRESHOLD.put("etri:06", Range.closed(36d, 40d));
		VALID_DISTANCE_THRESHOLD.put("etri:07", Range.closed(40d, 47d));

		String appId = envs.getOrDefault("KAFKA_APPLICATION_ID_CONFIG", "track-global-objects");
		String kafkaServers = envs.getOrDefault("KAFKA_BOOTSTRAP_SERVERS_CONFIG", "localhost:9092");
		String topicLocationEvents = envs.getOrDefault("DNA_TOPIC_TRACK_EVENTS", "node-tracks");
		String topicGlobalTracks = envs.getOrDefault("DNA_TOPIC_TRACK_CLUSTERS", "global-tracks");
		
		if ( s_logger.isInfoEnabled() ) {
			s_logger.info("Kafka servers: {}", kafkaServers);
			s_logger.info("TrackEvents topic: {}={}", "DNA_TOPIC_TRACK_EVENTS", topicLocationEvents);
			s_logger.info("GlobalTracks topic: {}={}", "DNA_TOPIC_GLOBAL_TRACKS", topicGlobalTracks);
		}
		
		Topology topology = new GlobalObjectTrackerTopologyBuilder(areas)
										.setTrackEventsTopic(topicLocationEvents)
										.setGlobalTracksTopic(topicGlobalTracks)
										.setValidDistanceThresholds(VALID_DISTANCE_THRESHOLD)
										.setOffsetReset(AutoOffsetReset.EARLIEST)
										.build();
		
		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ByteArraySerde.class);
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
		config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
//		config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
		
		KafkaStreams streams = new KafkaStreams(topology, config);
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
		
		streams.start();
	}
}
