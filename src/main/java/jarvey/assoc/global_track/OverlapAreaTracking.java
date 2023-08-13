package jarvey.assoc.global_track;

import static utils.Utilities.checkArgument;
import static utils.Utilities.checkState;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.locationtech.jts.geom.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;

import jarvey.streams.HoppingWindowManager;
import jarvey.streams.Window;
import jarvey.streams.model.GlobalTrack;
import jarvey.streams.model.LocalTrack;
import jarvey.streams.model.NodeTrack;
import jarvey.streams.model.TrackletId;
import utils.Utilities;
import utils.func.FOption;
import utils.func.Tuple;
import utils.stream.FStream;
import utils.stream.KeyedGroups;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class OverlapAreaTracking {
	private static final Logger s_logger = LoggerFactory.getLogger(OverlapAreaTracking.class);
	
	private static final Range<Double> DEFAULT_DISTANCE_THRESHOLD = Range.closed(47d, 50d);
	private static final float DEFAULT_DISTANCE_DISTANCE = 6f;
	private static final Duration DEFAULT_TTL = Duration.ofSeconds(3);
	
	private final String m_areaId;
	private final List<TrackCluster> m_clusters = Lists.newArrayList();
	private final Map<String,Range<Double>> m_distThresholds = Maps.newHashMap();
	private float m_distinctDist = DEFAULT_DISTANCE_DISTANCE;
	private long m_ageThresholdMs = DEFAULT_TTL.toMillis();
	private final HoppingWindowManager m_windowMgr;
	
	public static OverlapAreaTracking ofWindowSize(String id, Duration size) {
		return new OverlapAreaTracking(id, size);
	}
	
	private OverlapAreaTracking(String id, Duration windowSize) {
		m_areaId = id;
		m_windowMgr = HoppingWindowManager.ofWindowSize(windowSize);
	}
	
	public OverlapAreaTracking setValidDistanceThresholds(Map<String,Range<Double>> thresholds) {
		m_distThresholds.putAll(thresholds);
		return this;
	}
	
	public OverlapAreaTracking setDistinctThreshold(float distance) {
		m_distinctDist = distance > 0 ? distance : DEFAULT_DISTANCE_DISTANCE;
		
		return this;
	}
	
	public OverlapAreaTracking setAgeThreshold(long ageMillis) {
		m_ageThresholdMs = ageMillis;
		return this;
	}
	
	public List<GlobalTrack> track(NodeTrack track) {
		long ts = track.getTimestamp();
		
		Tuple<List<Window>, List<Window>> result = m_windowMgr.collect(ts);
		Utilities.checkState(result._1.size() <= 1);
		Utilities.checkState(result._2.size() <= 1);
		
		List<GlobalTrack> trackeds = Collections.emptyList();
		if ( result._1.size() > 0 ) {
			long maxTs = FStream.from(result._1).mapToLong(w -> w.endTime()).max();
			trackeds = FStream.from(m_clusters)
								.filter(c -> c.getTimestamp() < maxTs)
								.flatMap(this::toGlobalTracks)
								.toList();
		}
		
		if ( result._2.size() > 0 ) {
			update(track);
		}
		
		return trackeds;
	}
	
	private FStream<GlobalTrack> toGlobalTracks(TrackCluster cluster) {
		Collection<LocalTrack> ltracks = cluster.getLocalTracks();
		return FStream.from(ltracks)
						.map(lt -> new GlobalTrack(lt.getTrackletId(), cluster.getOverlapAreaId(),
													cluster.getMeanLocation(),
													Lists.newArrayList(cluster.getLocalTracks()),
													cluster.getTimestamp()));
	}
	
	private FOption<TrackCluster> update(NodeTrack track) {
		purgeOldTrackCluster(track.getTimestamp());
		
		// track이 포함된 global location을 검색한다.
        FOption<TrackCluster> ogloc = getTrackCluster(track.getTrackletId());
        
        // track이 종료된 경우는 별도로 처리한다.
        if ( track.isDeleted() ) {
        	if ( ogloc.isPresent() ) {
        		removeTrack(ogloc.get(), track.getNodeId());
        	}
        	return ogloc;
        }
        
        Range<Double> threshold = m_distThresholds.getOrDefault(track.getNodeId(),
        														DEFAULT_DISTANCE_THRESHOLD);

        // track의 카메라로부터의 거리 (distance)가 일정 거리 (exit_range)보다
        // 먼 경우 locator에서 무조건 삭제시킨다.
        if ( ogloc.isPresent() && track.getDistance() > threshold.upperEndpoint() ) {
        	removeTrack(ogloc.get(), track.getNodeId());
            return ogloc;
        }

        // track이 등록되어 있지 않은 상태에서 카메라로부터의 거리 (distance)가
        // 일정 거리 (enter_range)보다 먼 경우는 무시한다.
        if ( ogloc.isAbsent() && track.getDistance() > threshold.lowerEndpoint() ) {
            return FOption.empty();
        }
        
        LocalTrack lstrack = LocalTrack.from(track);
		return FOption.of(assign(lstrack, m_clusters));
	}
	
	@Override
	public String toString() {
		return m_clusters.toString();
	}
	
	public FStream<Tuple<TrackCluster,LocalTrack>> sourceTracks() {
		return FStream.from(m_clusters)
						.flatMap(cluster -> FStream.from(cluster.getLocalTracks())
													.map(t -> Tuple.of(cluster, t)));
	}
	
	private TrackCluster assign(LocalTrack ltrack, List<TrackCluster> clusters) {
		Point loc = ltrack.getLocation();
		
		// 이미 등록된 모든 global location들과의 거리를 계산하여, track과 일정 거리 내에 있는
		// global location들을 검색하여 그 거리 값을 기준으로 정렬한다.
		// 만일 일정 거리 내에 있는 global location에 동일 node의 다른 track이 존재하는 경우에는
		// 그 track의 해당 global location과의 거리보다 짧은 경우만 포함시킨다.
		List<TrackCluster> sortedCloseGlocs
				= FStream.from(clusters)
							.filter(gloc -> gloc.distance(loc) <= m_distinctDist)
							.filter(gloc -> this.isCloserThanCompetitor(gloc, ltrack))
							.sort((g1, g2) -> Double.compare(g2.distance(loc),  g1.distance(loc)))
							.toList();
		if ( !sortedCloseGlocs.isEmpty() ) {
			return select(ltrack, sortedCloseGlocs);
		}

		// track과 근접한 global location이 없는 경우 새 global location을 생성한다.
		
		// 만일 track이 frame에 따라 위치가 급격히 변한 경우에는 동일 track을 포함하는 global location이
		// 존재할 수도 있기 때문에 삭제를 해본다.
        FOption<TrackCluster> oprevGloc = getTrackCluster(ltrack.getTrackletId());
        if ( oprevGloc.isPresent() ) {
        	TrackCluster prevGloc = oprevGloc.get();
            if ( prevGloc.size() == 1 ) {
            	prevGloc.update(ltrack);
                return prevGloc;
            }
            else {
            	removeTrack(prevGloc, ltrack.getNodeId());
            }
        }
        
		TrackCluster newCluster = new TrackCluster(m_areaId, ltrack);
		
		// 다른 global location에 포함된 다른 node들의 track 중에서 새로 만들어진 global location과
		// 더 가까운 track이 있는가 조사하여 이들을 새로 만들어진 gloc으로 re-assign 시킨다.
		KeyedGroups<String, Tuple<TrackCluster,LocalTrack>> groups = 
			sourceTracks()
				// 다른 node에서 생성된 track만 선택한다. 본 node의 track은 'track'이 포함될 것이기 때문.
				.filterNot(t -> ltrack.getNodeId().equals(t._2.getNodeId()))
				// 선택된 track, track이 속했던 global location을 tuple로 만듬.
				// 결과: <prev_gloc, track> 형태의 tuple을 가지게 됨.
				.map(t -> Tuple.of(t._1, t._2))
				// 선택된 track 중에서 그것의 속한 gloc과의 거리보다 새 newGloc과의 거리가
				// 더 가까운 경우만 선택
				.filter(t -> newCluster.distance(t._2) < t._1.distance(t._2))
				.groupByKey(t -> t._2.getNodeId());
		
		Comparator<Tuple<TrackCluster,LocalTrack>> distCmp
			= (t1, t2) -> Double.compare(newCluster.distance(t2._2), newCluster.distance(t1._2));
		groups.stream()
			.map((k, grp) -> Collections.min(grp, distCmp))
			.forEach(t -> {
				removeTrack(t._1, t._2.getNodeId());
				newCluster.update(t._2);
			});
		m_clusters.add(newCluster);
		
		return newCluster;
	}
	
	private TrackCluster select(LocalTrack ltrack, List<TrackCluster> validGLocs) {
		checkArgument(validGLocs.size() > 0, "empty valid GlobalLocations");
		
		TrackCluster prevCluster = getTrackCluster(ltrack.getTrackletId()).getOrNull();
		
		TrackCluster closestCluster = validGLocs.get(0);
		FOption<LocalTrack> oprevTrack = closestCluster.get(ltrack.getNodeId());
		if ( oprevTrack.isAbsent() ) {
			// 가장 가까운 gloc에 동일 node에서 생성된 track이 존재하지 않는 경우
			// 이 gloc에 track을 포함시킨다.
			
			// 만일 track이 기존 다른 gloc에 포함된 상태이면 그곳에서 제외시킨다.
			if ( prevCluster != null && closestCluster != prevCluster ) {
				removeTrack(prevCluster, ltrack.getNodeId());
			}
			closestCluster.update(ltrack);
		}
		else {
			// 가장 가까운 gloc에 이미 동일 node에서 생성된 track이 존재하는 경우
			LocalTrack prevTrack = oprevTrack.get();
			
			if ( ltrack.isSameTrack(prevTrack) ) {
				// 하나의 track은 최대 1개의 global location에 등록되기 때문에,
				// prevGloc과 closest는 동일해야 한다.
				checkState(prevCluster.equals(closestCluster));
				
	            // 가장 가까운 gloc에 이미 동일 track이 존재하는 경우
				if ( closestCluster.size() == 1 && validGLocs.size() >= 2 ) {
	                // 만일 자신이 포함된 gloc이 자신만으로 구성된 location이고, 
	                // 다른 가까운 gloc이 존재하는 경우 해당 gloc과 merge를 시도한다.
					removeTrack(prevCluster, ltrack.getNodeId());
					return select(ltrack, validGLocs.subList(1, validGLocs.size()));
				}
	            closestCluster.update(ltrack);
			}
			else {
				// 가장 가까운 gloc에 동일 node에서 발생된 다른 track (=prevTrack)이 존재하는 경우.
				//
				checkState(closestCluster.distance(ltrack) < closestCluster.distance(prevTrack),
						() -> String.format("should: %.2f < %.2f",
											closestCluster.distance(ltrack), closestCluster.distance(prevTrack)));
				
				// 대상 track이 이미 다른 gloc에 등록된 상태라면, 그 gloc에서 제외시킨다.
				if ( prevCluster != null && !closestCluster.equals(prevCluster) )  {
					removeTrack(prevCluster, ltrack.getNodeId());
				}
				closestCluster.update(ltrack);
				
				// 이전 track은 적절한 global location에 등록시킨다.
				// 'closest.update()' 함수 호출에 의해 이전 global location에서는 자동적으로 제외된다.
				assign(prevTrack, m_clusters);
			}
		}
		
		return closestCluster;
	}
	
	private FOption<TrackCluster> getTrackCluster(TrackletId tid) {
		return FStream.from(m_clusters).findFirst(c -> c.exists(tid));
	}
	
	private void removeTrack(TrackCluster cluster, String nodeId) {
		cluster.remove(nodeId);
		
		// 제거 후 global location이 empty인 경우에는 그 global location도 함께 제거한다.
		if ( cluster.size() == 0 ) {
			m_clusters.remove(cluster);
		}
	}
	
	private boolean isCloserThanCompetitor(TrackCluster gloc, LocalTrack track) {
		LocalTrack competitor = gloc.get(track.getNodeId()).getOrNull();
		if ( competitor == null || competitor.getTrackId().equals(track.getTrackId()) ) {
			return true;
		}
		else if ( gloc.size() == 1 ) {
			// competitor가 해당 global location의 유일한 contribution인 경우는 제외시킨다.
			return false;
		}
		else {
			return gloc.distance(track.getLocation()) < gloc.distance(competitor.getLocation());
		}
	}
	
	private void purgeOldTrackCluster(long now) {
		List<TrackCluster> oldClusters =  FStream.from(m_clusters)
												.filter(c -> (now - c.getTimestamp()) >= m_ageThresholdMs)
												.toList();
		for ( TrackCluster cluster: oldClusters ) {
			if ( s_logger.isInfoEnabled() ) {
				Duration age = Duration.ofMillis(now - cluster.getTimestamp());
				s_logger.info("purge too old track-cluster: {}, age={}", cluster, age);
			}
			m_clusters.remove(cluster);
		}
	}
}
