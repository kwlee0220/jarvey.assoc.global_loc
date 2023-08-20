package jarvey.assoc.global_track;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Range;

import jarvey.assoc.OverlapArea;
import jarvey.assoc.OverlapAreaRegistry;
import jarvey.streams.model.GlobalTrack;
import jarvey.streams.node.NodeTrack;
import jarvey.streams.serialization.json.GsonUtils;
import utils.UnitUtils;
import utils.func.Unchecked;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestGlobalObjectTracker {
	private static final Logger s_logger = LoggerFactory.getLogger(TestGlobalObjectTracker.class.getPackage().getName());
	
	private static final Map<String,Range<Double>> VALID_DISTANCE_THRESHOLD = Maps.newHashMap();
	static {
		VALID_DISTANCE_THRESHOLD.put("etri:04", Range.closed(37d, 50d));
		VALID_DISTANCE_THRESHOLD.put("etri:05", Range.closed(50d, 60d));
		VALID_DISTANCE_THRESHOLD.put("etri:06", Range.closed(36d, 40d));
		VALID_DISTANCE_THRESHOLD.put("etri:07", Range.closed(40d, 47d));
	}
	
	public static void main(String... args) throws Exception {
		Map<String,String> envs = System.getenv();
		
		String topic = "node-tracks";
		Duration pollTimeout = Duration.ofMillis(1000);
		
		OverlapAreaRegistry areas = new OverlapAreaRegistry();
		OverlapArea etriTestbed = new OverlapArea("etri_testbed");
		etriTestbed.addOverlap("etri:04", "etri:05");
		etriTestbed.addOverlap("etri:04", "etri:06");
		etriTestbed.addOverlap("etri:05", "etri:06");
		etriTestbed.addOverlap("etri:05", "etri:07");
		areas.add("etri_testbed", etriTestbed);
		
		Properties props = new Properties();
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_global_locator");
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, (int)UnitUtils.parseDuration("10s"));
		props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, (int)UnitUtils.parseDuration("30s"));

		final KafkaConsumer<String, Bytes> consumer = new KafkaConsumer<>(props);
		
		Thread mainThread = Thread.currentThread();
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				try {
					consumer.wakeup();
					mainThread.join();
				}
				catch ( InterruptedException e ) {
					s_logger.error("interrupted", e);
				}
			}
		});
		
		driver(consumer, topic, pollTimeout, areas);
	}
	
	public static void driver(KafkaConsumer<String, Bytes> consumer, String topic, Duration pollTimeout,
								OverlapAreaRegistry areas) {
		GlobalObjectTracker gtracker = GlobalObjectTracker.builder()
														.setOverlapAreaRegistry(areas)
														.build();
		try {
			consumer.subscribe(Arrays.asList(topic));
			
			Serde<NodeTrack> serde = GsonUtils.getSerde(NodeTrack.class);
			Deserializer<NodeTrack> deser = serde.deserializer();
			
			long last_ts = -1;
			while ( true ) {
				ConsumerRecords<String,Bytes> records = consumer.poll(pollTimeout);
				for ( ConsumerRecord<String, Bytes> record: records ) {
					NodeTrack track = deser.deserialize(record.topic(), record.value().get());
					
//					if ( track.getTimestamp() < last_ts ) {
//						System.out.println(track);
//					}
//					last_ts = track.getTimestamp();
					
					for ( GlobalTrack gtrack: gtracker.track(track) ) {
						System.out.println(gtrack);
					}
				}
			}
		}
		catch ( WakeupException ignored ) { }
		catch ( Exception e ) {
			s_logger.error("Unexpected error", e);
		}
		finally {
			Unchecked.runOrIgnore(consumer::close);
		}
	}
}
