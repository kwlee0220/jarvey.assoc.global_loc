package jarvey.assoc.global_track;

import java.time.Duration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import jarvey.streams.HoppingWindowManager;
import jarvey.streams.Window;
import jarvey.streams.model.GlobalTrack;
import jarvey.streams.model.LocalTrack;
import jarvey.streams.node.NodeTrack;

import utils.Utilities;
import utils.func.Tuple;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class NoneOverlapAreaTracking {
	private static final Logger s_logger = LoggerFactory.getLogger(NoneOverlapAreaTracking.class);
	
	private final List<NodeTrack> m_tracks = Lists.newArrayList();
	private final HoppingWindowManager m_windowMgr;
	
	public static NoneOverlapAreaTracking ofWindowSize(Duration size) {
		return new NoneOverlapAreaTracking(size);
	}
	
	private NoneOverlapAreaTracking(Duration windowSize) {
		m_windowMgr = HoppingWindowManager.ofWindowSize(windowSize);
	}
	
	public List<GlobalTrack> track(NodeTrack track) {
		m_tracks.add(track);
		
		long ts = track.getTimestamp();
		Tuple<List<Window>, List<Window>> result = m_windowMgr.collect(ts);
		Utilities.checkState(result._1.size() <= 1);
		Utilities.checkState(result._2.size() <= 1);
		
		long endTs = FStream.from(result._1).mapToLong(w -> w.endTime()).max();
		List<NodeTrack> expireds = FStream.from(m_tracks)
											.filter(t -> t.getTimestamp() < endTs)
											.toList();
		m_tracks.removeAll(expireds);
		
		return FStream.from(expireds)
						.map(LocalTrack::from)
						.map(GlobalTrack::new)
						.toList();
	}
	
	@Override
	public String toString() {
		return m_windowMgr.toString();
	}
}
