package net.butfly.albacore.calculus.datasource;

import java.io.Serializable;

import net.butfly.albacore.calculus.functor.Functor.Type;

public class Detail implements Serializable {
	private static final long serialVersionUID = 1900035964021610093L;
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

	@Override
	public String toString() {
		switch (type) {
		case HBASE:
			return "[Table: " + hbaseTable + "]";
		case KAFKA:
			return "[Table: " + String.join(",", kafkaTopics) + "]";
		case MONGODB:
			return "[Table: " + mongoTable + ", Filter: " + mongoFilter + "]";
		default:
			return "";
		}
	}
}
