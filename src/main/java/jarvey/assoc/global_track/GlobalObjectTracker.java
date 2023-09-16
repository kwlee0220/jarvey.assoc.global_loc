package jarvey.assoc.global_track;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Range;

import jarvey.assoc.OverlapArea;
import jarvey.assoc.OverlapAreaRegistry;
import jarvey.streams.model.GlobalTrack;
import jarvey.streams.node.NodeTrack;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GlobalObjectTracker {
	private static final Logger s_logger = LoggerFactory.getLogger(GlobalObjectTracker.class);
	private static final Duration DEFAULT_WINDOW_SIZE = Duration.ofMillis(100);
	
	private final OverlapAreaRegistry m_registry;
	private final Duration m_windowSize;
	private final Map<String,Range<Double>> m_validThresholds = Maps.newHashMap();

	private final Map<String,OverlapAreaTracking> m_trackings = Maps.newHashMap();
	private final NoneOverlapAreaTracking m_nonOvTracking;
	
	private GlobalObjectTracker(Builder builder) {
		m_registry = builder.m_registry;
		m_windowSize = builder.m_windowSize;
		
		m_nonOvTracking = NoneOverlapAreaTracking.ofWindowSize(builder.m_windowSize);
	}
	
	public List<GlobalTrack> track(NodeTrack track) {
		OverlapArea area = m_registry.findByNodeId(track.getNodeId());
		if ( area != null ) {
			return handleOverlapAreaTrack(area.getId(), track);
		}
		else {
			return m_nonOvTracking.track(track);
		}
	}
	
	private List<GlobalTrack> handleOverlapAreaTrack(String ovId, NodeTrack track) {
		OverlapAreaTracking tracking = m_trackings.get(ovId);
		if ( tracking == null ) {
			tracking = OverlapAreaTracking.ofWindowSize(ovId, m_windowSize)
											.setValidDistanceThresholds(m_validThresholds);
			m_trackings.put(ovId, tracking);
		}
		
		return tracking.track(track);
	}
	
	public static Builder builder() {
		return new Builder();
	}
	public static class Builder {
		private OverlapAreaRegistry m_registry = new OverlapAreaRegistry();
		private Map<String,Range<Double>> m_validThresholds = Maps.newHashMap();
		private Duration m_windowSize = DEFAULT_WINDOW_SIZE;
		
		private Builder() {
		}
		
		public GlobalObjectTracker build() {
			return new GlobalObjectTracker(this);
		}
		
		public Builder setOverlapAreaRegistry(OverlapAreaRegistry registry) {
			m_registry = registry;
			return this;
		}
		
		public Builder setWindowSize(Duration size) {
			m_windowSize = size;
			return this;
		}
		
		public Builder setValidDistanceThresholds(Map<String,Range<Double>> distThresholds) {
			m_validThresholds.putAll(m_validThresholds);
			return this;
		}
	}
}