package kr.ac.hongik.apl.Generator;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Generator {
	final long duration = 300000;
	final int fakeRouteSize = 100;
	private boolean isDataLoop;
	private int index;

	private String user_id;
	private String verification_method;
	private String car_id;
	private long mileage;
	private long timestamp;
	private double fuel_level;
	private List<List<Double>> route;

	public Generator(String initDataPath, boolean isDataLoop) {
		try {
			SimpleDateFormat humanTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			ObjectMapper objectMapper = new ObjectMapper()
					.enable(JsonParser.Feature.ALLOW_COMMENTS)
					.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
					.setDateFormat(humanTimeFormat);

			Map initMap = objectMapper.readValue(this.getClass().getResourceAsStream(initDataPath), Map.class);
			initMap.computeIfAbsent("route", k -> makeRandomRoute(fakeRouteSize));

			JsonNode tree = objectMapper.valueToTree(initMap);

			this.isDataLoop = isDataLoop;
			this.index = 0;
			this.user_id = tree.get("user_id").asText();
			this.verification_method = tree.get("verification_method").asText();
			this.car_id = tree.get("car_id").asText();
			this.mileage = tree.get("mileage").asLong();
			this.route = objectMapper.readValue(tree.get("route").toString(), new TypeReference<List<List<Double>>>() {});
			this.fuel_level =  tree.get("fuel_level").asDouble();
			this.timestamp = humanTimeFormat.parse((String) initMap.get("timestamp")).getTime() - duration;
		} catch (ParseException | IOException wrap) {
			throw new RuntimeException(wrap);
		}
	}

	public Pair<LinkedHashMap<String, Object>, LinkedHashMap<String, Object>> generate() throws NoSuchFieldException {
		if (index == route.size()) {
			if (isDataLoop)
				this.index = 0;
			else
				throw new NoSuchFieldException();
		}
		double distance = (index == 0) ? 0 : getDistance(route.get(index - 1), route.get(index));
		this.fuel_level -= 0.1;
		this.timestamp += duration;
		this.mileage += distance;
		SimpleDateFormat humanTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		LinkedHashMap<String, Object> car_log = new LinkedHashMap<String, Object>();
		car_log.put("car_id", car_id);
		car_log.put("timestamp", humanTimeFormat.format(new Date(timestamp)));
		car_log.put("fuel_level", fuel_level);
		car_log.put("car_location", route.get(index));
		car_log.put("mileage", mileage);

		LinkedHashMap<String, Object> user_log = new LinkedHashMap<String, Object>();
		user_log.put("user_id", user_id);
		user_log.put("login_time", humanTimeFormat.format(new Date(timestamp)));
		user_log.put("verification_method", verification_method);
		user_log.put("car_id", car_id);

		this.index++;
		return new ImmutablePair<>(car_log, user_log);
	}

	/**
	 * @param size 생성할 리스트의 총 길이
	 * @return 메서드에 표기된 위/경도 범위에 따른 랜덤 좌표 리스트
	 */
	private List<List<Double>> makeRandomRoute(int size) {
		Random random = new Random();
		List<Double> latitude = random.doubles(size, 37.5, 37.57)
				.boxed().collect(Collectors.toList());
		List<Double> longitude = random.doubles(size, 126.8, 127.0)
				.boxed().collect(Collectors.toList());

		List<List<Double>> result = new ArrayList<>();
		IntStream.range(0, size)
				.forEach(i -> result.add(Arrays.asList(latitude.get(i), longitude.get(i))));

		return result;
	}

	/**
	 * @param p1 출발 지점 좌표
	 * @param p2 도착 지점 좌표
	 * @return 좌표 사이의 직선 거리(km)
	 */
	private double getDistance(List p1, List p2) {
		double lat1 = (double) p1.get(0);
		double lon1 = (double) p1.get(1);
		double lat2 = (double) p2.get(0);
		double lon2 = (double) p2.get(1);

		double theta = lon1 - lon2;
		double distance = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2))
				+ Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2))
				* Math.cos(deg2rad(theta));
		distance = Math.acos(distance);
		distance = rad2deg(distance);
		distance = distance * 60 * 1.1515;
		distance = distance * 1.609344;
		return (distance);
	}

	private double rad2deg(double rad) {
		return (rad * 180.0 / Math.PI);
	}

	private double deg2rad(double deg) {
		return (deg * Math.PI / 180.0);
	}
}
