package net.butfly.albacore.calculus;

import java.io.Serializable;

import org.apache.spark.streaming.api.java.AbstractJavaDStreamLike;

public class FunctorConfig implements Serializable {
	private static final long serialVersionUID = 5323846657146326084L;
	public Class<?> functorClass;
	public String datasource;
	// spark conf
	public AbstractJavaDStreamLike<?, ?, ?> dstream;
	// hbase conf
	public String hbaseTable;
	// kafka
	public String[] kafkaTopics;
	// mongodb
	public String mongoTable;
	public String mongoFilter;
}
