package net.butfly.albacore.calculus;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.spark.streaming.api.java.AbstractJavaDStreamLike;
import org.jongo.MongoCollection;

import net.butfly.albacore.calculus.marshall.Marshaller;

public class FunctorConfig {
	public Class<?> functorClass;
	public String datasource;
	public AbstractJavaDStreamLike<?, ?, ?> dstream;
	public Marshaller<?, ?> marshaller;
	// db conf
	public Configuration hconf, mconf;
	public Connection hconn;
	public TableName htname;
	// kafka
	public String[] kafkaTopics;
	// mongodb
	public MongoCollection mcol;
}
