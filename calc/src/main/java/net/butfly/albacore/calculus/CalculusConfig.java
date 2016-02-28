package net.butfly.albacore.calculus;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.AbstractJavaDStreamLike;

import net.butfly.albacore.calculus.marshall.Marshaller;

public class CalculusConfig {
	public Class<?> functorClass;
	public AbstractJavaDStreamLike<?, ?, ?> stream;
	// db conf
	public Configuration hconf, mconf;
	public Connection hconn;
	public TableName htname;
	public Marshaller<?> marshaller;
	public StructType schema;
	// kafka
	public String[] kafkaTopics;
}
