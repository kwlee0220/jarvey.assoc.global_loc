package jarvey.assoc.global_track;

import java.util.Collection;
import java.util.Map;

import org.locationtech.jts.geom.Point;

import com.google.common.collect.Maps;

import jarvey.streams.model.LocalTrack;
import jarvey.streams.model.Timestamped;
import jarvey.streams.model.TrackletId;
import utils.func.FOption;
import utils.func.Funcs;
import utils.geo.util.GeoUtils;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
final class TrackCluster implements Timestamped {
	private String m_ovId;
	private Point m_meanLocation;
	private final Map<String,LocalTrack> m_ltracks;
	private long m_ts;
	
	TrackCluster(String ovId, LocalTrack initialTrack) {
		m_ovId = ovId;
		m_meanLocation = initialTrack.getLocation();
		m_ltracks = Maps.newHashMap();
		m_ltracks.put(initialTrack.getNodeId(), initialTrack);
		m_ts = initialTrack.getTimestamp();
	}
	
	public String getOverlapAreaId() {
		return m_ovId;
	}
	
	public Collection<LocalTrack> getLocalTracks() {
		return m_ltracks.values();
	}
	
	public int size() {
		return m_ltracks.size();
	}
	
	public Point getMeanLocation() {
		return m_meanLocation;
	}
	
	public double distance(Point pt) {
		return pt.distance(getMeanLocation());
	}
	
	public double distance(LocalTrack track) {
		return track.getLocation().distance(getMeanLocation());
	}
	
	public long getTimestamp() {
		return m_ts;
	}
	
	public FOption<LocalTrack> get(String nodeId) {
		return FOption.ofNullable(m_ltracks.get(nodeId));
	}
	
	public boolean exists(TrackletId tracklet) {
		LocalTrack prev = m_ltracks.get(tracklet.getNodeId());
		return prev != null ? prev.getTrackletId().equals(tracklet) : false;
	}
	
	public void update(LocalTrack track) {
		m_ltracks.put(track.getNodeId(), track);
		m_ts = Math.max(m_ts, track.getTimestamp());
		
		// 기존 cache된 추정 위치 좌표를 무효화하여 다음번에 다시 계산되도록 한다.
		m_meanLocation = calcMeanLocation(m_ltracks.values());
	}
	
	public FOption<LocalTrack> remove(String nodeId) {
		LocalTrack removed = m_ltracks.remove(nodeId);
		if ( removed != null ) {
			m_meanLocation = calcMeanLocation(m_ltracks.values());
		}
		return FOption.ofNullable(removed);
	}
	
	@Override
	public String toString() {
		String tracksStr = FStream.from(m_ltracks.values())
								.map(t -> String.format("%s[%s]", t.getNodeId(), t.getTrackId()))
								.join('-');
		return String.format("%s#%d, {%s}", GeoUtils.toString(getMeanLocation(), 1), m_ts, tracksStr);
	}
	
	private static Point calcMeanLocation(Collection<LocalTrack> supports) {
		if ( supports.size() == 1 ) {
			return Funcs.getFirst(supports).getLocation();
		}
		else {
			double sum_x = 0;
			double sum_y = 0;
			int count = 0;
			for ( LocalTrack track: supports ) {
				sum_x += track.getLocation().getX();
				sum_y += track.getLocation().getY();
				++count;
			}
			
			return GeoUtils.toPoint(sum_x / count, sum_y / count);
		}
	}
}