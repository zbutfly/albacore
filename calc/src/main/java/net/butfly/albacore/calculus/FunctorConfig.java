package net.butfly.albacore.calculus;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.streaming.api.java.AbstractJavaDStreamLike;

import net.butfly.albacore.calculus.Functor.Type;

public class FunctorConfig implements Serializable {
	private static final long serialVersionUID = 5323846657146326084L;
	public Class<?> functorClass;
	// spark conf
	public AbstractJavaDStreamLike<?, ?, ?> dstream;

	// datasources (id key)
	public Map<String, Detail> stockingDSs = new HashMap<>();
	public Map<String, Detail> streamingDSs = new HashMap<>();
	public Map<String, Detail> savingDSs = new HashMap<>();

	public static class Detail {
		public Type type;
		// hbase conf
		public String hbaseTable;

		public Detail(String hbaseTable) {
			super();
			this.type = Type.HBASE;
			this.hbaseTable = hbaseTable;
		}

		// kafka
		public String[] kafkaTopics;

		public Detail(String... kafkaTopics) {
			super();
			this.type = Type.KAFKA;
			this.kafkaTopics = kafkaTopics;
		}

		// mongodb
		public String mongoTable;
		public String mongoFilter;

		public Detail(String mongoTable, String mongoFilter) {
			super();
			this.type = Type.MONGODB;
			this.mongoTable = mongoTable;
			this.mongoFilter = mongoFilter;
		}
	}
}
