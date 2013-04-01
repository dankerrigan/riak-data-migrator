package com.basho.proserv.datamigrator.riak;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.riak.client.bucket.BucketProperties;
import com.thoughtworks.xstream.XStream;

public class RiakBucketProperties {
	private Logger log = LoggerFactory.getLogger(RiakBucketProperties.class);
	private final Connection connection;
	public RiakBucketProperties(Connection connection) {
		this.connection = connection;
	}
	
	public BucketProperties getBucketProperties(String bucketName) {
		BucketProperties bucketProperties = null;
		try {
			bucketProperties = this.connection.riakClient.fetchBucket(bucketName);
		} catch (IOException e) {
			log.error("Coudldn't retrieve bucket", e);
			return null;
		}
		return bucketProperties;
	}
	
	public boolean setBucketProperties(String bucketName, BucketProperties bucketProperties) {
		boolean success = true;
		try {
			this.connection.riakClient.updateBucket(bucketName, bucketProperties);
		} catch (IOException e) {
			log.error("Couldn't set bucket properties for bucket " +  bucketName, e);
			success = false;
		}
		
		return success;
	}
	
	public boolean writeBucketProperties(BucketProperties props, File file) {
		XStream xStream = new XStream();
		FileWriter writer;
		try {
			writer = new FileWriter(file);
		} catch (IOException e) {
			log.error("Could not write bucket properties to file " + file.getAbsolutePath(), e);
			return false;
		}
		xStream.toXML(props, writer);
		try {
			writer.flush();
			writer.close();
		} catch (IOException e) {
			log.error("Could not close bucket properties file", e);
			return false;
		}
		return true;
	}
	
	public BucketProperties readBucketProperties(File file) {
		XStream xStream = new XStream();
		FileReader reader;
		try {
			reader = new FileReader(file);
		} catch (IOException e) {
			log.error("Could not open bucket properties file " + file.getAbsolutePath(), e);
			return null;
		}
		BucketProperties bucketProps = (BucketProperties)xStream.fromXML(reader);
		try {
			reader.close();
		} catch (IOException e) {
			log.error("Could not close bucket properties file", e);
			return null;
		}
		return bucketProps;
	}
	
	public static File createBucketSettingsFile(File basePath) {
		String filename = basePath.getAbsolutePath() + "/" + "bucketProps.xml";
		File xmlPath = new File(filename);
		return xmlPath;
	}
}
