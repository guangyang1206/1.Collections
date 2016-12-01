package com.epam.processor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.epam.data.RoadAccident;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

/**
 * This is to be completed by mentees
 */
public class DataProcessor {

	private final List<RoadAccident> roadAccidentList;

	public DataProcessor(List<RoadAccident> roadAccidentList) {
		this.roadAccidentList = roadAccidentList;
	}

	// First try to solve task using java 7 style for processing collections

	/**
	 * Return road accident with matching index
	 * 
	 * @param index
	 * @return
	 */
	public RoadAccident getAccidentByIndex7(String index) {
        for(RoadAccident roadAccident : roadAccidentList){
        	if(roadAccident.getAccidentId().equals(index)){
        		return roadAccident;
        	}
        }     
        return null;
	}

	/**
	 * filter list by longtitude and latitude values, including boundaries
	 * 
	 * @param minLongitude
	 * @param maxLongitude
	 * @param minLatitude
	 * @param maxLatitude
	 * @return
	 */
	public Collection<RoadAccident> getAccidentsByLocation7(float minLongitude, float maxLongitude, float minLatitude,
			float maxLatitude) {

		/*
		 * List<RoadAccident> roadAccidnts = new ArrayList<>();
		 * roadAccidnts.removeIf(s -> s.getLongitude() > maxLongitude);
		 * roadAccidnts.removeIf(s -> s.getLongitude() < minLongitude);
		 * roadAccidnts.removeIf(s -> s.getLatitude() > maxLatitude);
		 * roadAccidnts.removeIf(s -> s.getLatitude() < minLatitude);
		 * 
		 * return roadAccidnts;
		 */

		List<RoadAccident> roadAccidnts = new ArrayList<>();
		for (RoadAccident ra : roadAccidentList) {
			if (ra.getLongitude() >= minLongitude && ra.getLongitude() <= maxLongitude
					&& ra.getLatitude() >= minLatitude && ra.getLatitude() <= maxLatitude) {
				roadAccidnts.add(ra);
			}
		}

		return roadAccidnts;
	}

	/**
	 * count incidents by road surface conditions ex: wet -> 2 dry -> 5
	 * 
	 * @return
	 */
	public Map<String, Long> getCountByRoadSurfaceCondition7() {

		Map<String, Long> roadAccidentsCount = new HashMap<>();

		for (RoadAccident roadAccident : roadAccidentList) {
			Long count = 0L;
			if (roadAccidentsCount.get(roadAccident.getRoadSurfaceConditions()) != null) {
				count = roadAccidentsCount.get(roadAccident.getRoadSurfaceConditions());
			}
			roadAccidentsCount.put(roadAccident.getRoadSurfaceConditions(), ++count);
		}
		return roadAccidentsCount;
	}

	/**
	 * find the weather conditions which caused the top 3 number of incidents as
	 * example if there were 10 accidence in rain, 5 in snow, 6 in sunny and 1
	 * in foggy, then your result list should contain {rain, sunny, snow} - top
	 * three in decreasing order
	 * 
	 * @return
	 */
	@SuppressWarnings({ "rawtypes" })
	public List<String> getTopThreeWeatherCondition7() {

		Map<String, Long> mapbyWetherCondition = new HashMap<String, Long>();

		for (RoadAccident roadAccident : roadAccidentList) {
			Long count = 0L;
			if (mapbyWetherCondition.get(roadAccident.getWeatherConditions()) != null) {
				count = mapbyWetherCondition.get(roadAccident.getWeatherConditions());
			}
			mapbyWetherCondition.put(roadAccident.getWeatherConditions(), ++count);
		}
		List<Map.Entry<String, Long>> wetherConditionList = new LinkedList<>(mapbyWetherCondition.entrySet());
		Collections.sort(wetherConditionList, new Comparator<Map.Entry>() {
			@Override
			public int compare(Map.Entry o1, Map.Entry o2) {
				return ((Long)o2.getValue()).compareTo((Long)o1.getValue());
			}
		});

		List<String> weatherCondition = new ArrayList<String>();
		for (int i = 0; i < 3; i++) {
			Map.Entry entry = (Map.Entry) wetherConditionList.get(i);
			weatherCondition.add((String) entry.getKey());
		}
		return weatherCondition;

	}

	/**
	 * return a multimap where key is a district authority and values are
	 * accident ids ex: authority1 -> id1, id2, id3 authority2 -> id4, id5
	 * 
	 * @return
	 */
	public Multimap<String, String> getAccidentIdsGroupedByAuthority7() {
		Multimap<String, String> multiMap = ArrayListMultimap.create();
		for (RoadAccident roadAccident : roadAccidentList) {
			multiMap.put(roadAccident.getDistrictAuthority(), roadAccident.getAccidentId());
		}

		return multiMap;
	}

	// Now let's do same tasks but now with streaming api

	public RoadAccident getAccidentByIndex(String index) {
		return roadAccidentList.stream().filter(ra -> ra.getAccidentId().equals(index)).findFirst().orElse(null);
	}

	/**
	 * filter list by longtitude and latitude fields
	 * 
	 * @param minLongitude
	 * @param maxLongitude
	 * @param minLatitude
	 * @param maxLatitude
	 * @return
	 */
	public Collection<RoadAccident> getAccidentsByLocation(float minLongitude, float maxLongitude, float minLatitude,
			float maxLatitude) {

		List<RoadAccident> roadAccidents;
		roadAccidents = roadAccidentList.stream()
				.filter(ra -> ra.getLongitude() >= minLongitude && ra.getLongitude() <= maxLongitude)
				.filter(ra -> ra.getLatitude() >= minLatitude && ra.getLatitude() <= maxLatitude)
				.collect(Collectors.toList());
		return roadAccidents;
	}

	/**
	 * find the weather conditions which caused max number of incidents
	 * 
	 * @return
	 */
	public List<String> getTopThreeWeatherCondition() {
		Map<String, Long> map = roadAccidentList.stream().map(RoadAccident::getWeatherConditions)
				.collect(Collectors.groupingBy(item -> item, Collectors.counting()));
		return map.entrySet().stream().sorted((val1, val2) -> val2.getValue().compareTo(val1.getValue()))
				.map(Map.Entry::getKey).limit(3).collect(Collectors.toList());
	}

	/**
	 * count incidents by road surface conditions
	 * 
	 * @return
	 */
	public Map<String, Long> getCountByRoadSurfaceCondition() {
		Map<String, Long> countedRoadAccidents = roadAccidentList.stream().map(RoadAccident::getRoadSurfaceConditions)
				.collect(Collectors.groupingBy(item -> item, Collectors.counting()));	
		return countedRoadAccidents;
	}

	/**
	 * To match streaming operations result, return type is a java collection
	 * instead of multimap
	 * 
	 * @return
	 */
	public Map<String, List<String>> getAccidentIdsGroupedByAuthority() {
		return roadAccidentList.stream().collect(Collectors.groupingBy(RoadAccident::getDistrictAuthority,
				Collectors.mapping(RoadAccident::getAccidentId, Collectors.toList())));
	}

}
