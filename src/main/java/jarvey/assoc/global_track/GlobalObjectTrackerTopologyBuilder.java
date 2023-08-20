package jarvey.assoc.global_track;


import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;

import com.google.common.collect.Maps;
import com.google.common.collect.Range;

import jarvey.assoc.OverlapAreaRegistry;
import jarvey.streams.TrackTimestampExtractor;
import jarvey.streams.model.GlobalTrack;
import jarvey.streams.node.NodeTrack;
import jarvey.streams.serialization.json.GsonUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
final class GlobalObjectTrackerTopologyBuilder {
	private static final String TOPIC_GLOBAL_TRACKS = "global-tracks";
	private static final AutoOffsetReset DEFAULT_OFFSET_RESET = AutoOffsetReset.LATEST;
	private static final TrackTimestampExtractor TS_EXTRACTOR = new TrackTimestampExtractor();
	
	private final OverlapAreaRegistry m_areas;
	private final Map<String,Range<Double>> m_validThresholds = Maps.newHashMap();
	private String m_topicTrackEvents = "node-tracks";
	private AutoOffsetReset m_offsetReset = DEFAULT_OFFSET_RESET;
	private String m_globalTracksTopic = TOPIC_GLOBAL_TRACKS;
	private Duration m_windowSize = Duration.ofMillis(100);
	private int m_areaPartitionCount = 3;
	
	GlobalObjectTrackerTopologyBuilder(OverlapAreaRegistry areas) {
		m_areas = areas;
	}
	
	public GlobalObjectTrackerTopologyBuilder setValidDistanceThresholds(Map<String,Range<Double>> validThresholds) {
		m_validThresholds.putAll(validThresholds);
		return this;
	}
	
	public GlobalObjectTrackerTopologyBuilder setTrackEventsTopic(String topic) {
		Objects.requireNonNull(topic, "LocationEventsTopic is null");
		
		m_topicTrackEvents = topic;
		return this;
	}
	
	public GlobalObjectTrackerTopologyBuilder setOffsetReset(AutoOffsetReset offsetReset) {
		m_offsetReset = offsetReset;
		return this;
	}
	
	public GlobalObjectTrackerTopologyBuilder setGlobalTracksTopic(String topic) {
		m_globalTracksTopic = topic;
		return this;
	}
	
	private String assignOverlapAreaKey(String key, NodeTrack track) {
		return m_areas.findByNodeId(track.getNodeId())
						.map(ov -> ov.getId())
						.getOrElse(() -> track.getKey());
	}
	
//	private int assignPartitionId(String topic, String key, NodeTrack track, int nparts) {
//		String id =  m_areas.findByNodeId(track.getNodeId())
//							.map(ov -> ov.getId())
//							.getOrElse(() -> track.getId());
//		return id.hashCode() % nparts;
//	}
	
	public Topology build() {
		Objects.requireNonNull(m_topicTrackEvents, "track-events topic is null");
		
		GlobalObjectTracker m_tracker = GlobalObjectTracker.builder()
														.setOverlapAreaRegistry(m_areas)
														.setWindowSize(m_windowSize)
														.setValidDistanceThresholds(m_validThresholds)
														.build();
		
		StreamsBuilder builder = new StreamsBuilder();
		
		builder
			.stream(m_topicTrackEvents,
					Consumed.with(Serdes.String(), GsonUtils.getSerde(NodeTrack.class))
							.withName("source-track-events")
							.withTimestampExtractor(TS_EXTRACTOR)
							.withOffsetResetPolicy(m_offsetReset))
			.selectKey(this::assignOverlapAreaKey, Named.as("assign-overlap-area-key"))
			.repartition(Repartitioned.with(Serdes.String(), GsonUtils.getSerde(NodeTrack.class))
									.withName("repartition_by_overlap_area"))
//									.withNumberOfPartitions(m_areaPartitionCount)
//									.withStreamPartitioner(this::assignPartitionId))
			.flatMapValues(t -> m_tracker.track(t), Named.as("to_global_tracks"))
			.selectKey((k,gt) -> gt.getNodeId())
			.to(m_globalTracksTopic,
				Produced.with(Serdes.String(), GsonUtils.getSerde(GlobalTrack.class))
						.withName("sink-global-objects"));
		
		return builder.build();
	}
}
