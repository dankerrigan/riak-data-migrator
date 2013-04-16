package com.basho.proserv.datamigrator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Summary {
	// keeping bucket names separate to preserve order
	private final List<String> bucketNames = new ArrayList<String>();
	private final Map<String, Long> countStatistic = new HashMap<String, Long>();
	private final Map<String, Long> timeStatistic = new HashMap<String, Long>();
	private final Map<String, Long> sizeStatistic = new HashMap<String, Long>();
	private final Map<String, Long> valueErrorStatistic = new HashMap<String, Long>();
	
	public void addStatistic(String bucket, Long objectCount, Long time, Long size, Long valueErrors) {
		if (bucket == null || bucket.isEmpty()) {
			throw new IllegalArgumentException("bucketName cannot be null");
		}
		if (objectCount == null) {
			throw new IllegalArgumentException("objectCount cannot be null");
		}
		if (time == null) {
			throw new IllegalArgumentException("time cannot be null");
		}
		this.bucketNames.add(bucket);
		this.countStatistic.put(bucket, objectCount);
		this.timeStatistic.put(bucket, time);
		this.sizeStatistic.put(bucket, size);
		this.valueErrorStatistic.put(bucket, valueErrors);
	}
	
	public Long[] getBucketStatistic(String bucketName) {
		Long[] result = new Long[4];
		
		Long count = this.countStatistic.get(bucketName);
		Long time = this.timeStatistic.get(bucketName);
		Long size = this.sizeStatistic.get(bucketName);
		Long valueErrors = this.valueErrorStatistic.get(bucketName);

		result[0] = count;
		result[1] = time;
		result[2] = size;
		result[3] = valueErrors;
		
		return result;
	}
	
	public Long getTotalCount() {
		Long acc = 0l;
		for (String key : this.bucketNames) {
			acc += this.countStatistic.get(key);
		}
		return acc;
	}
	
	public Long getTotalTime() {
		Long acc = 0l;
		for (String key : this.bucketNames) {
			acc += this.timeStatistic.get(key);
		}
		return acc;
	}
	
	public Long getTotalSize() {
		Long acc = 0l;
		for (String key : this.bucketNames) {
			acc += this.sizeStatistic.get(key);
		}
		return acc;
	}
	
	public Long getTotalValueErrors() {
		Long acc = 0l;
		for (String key : this.bucketNames) {
			acc += this.valueErrorStatistic.get(key);
		}
		return acc;
	}
	
	public List<String> bucketNames() {
		return this.bucketNames;
	}
	
	public Map<String, Long[]> getStatistics() {
		Map<String, Long[]> stats = new HashMap<String, Long[]>();
		for (String bucketName : this.bucketNames) {
			stats.put(bucketName, this.getBucketStatistic(bucketName));
		}
		return stats;
	}
}
