package net.butfly.albacore.calculus;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.streaming.api.java.AbstractJavaDStreamLike;

import net.butfly.albacore.calculus.Calculating.Mode;
import net.butfly.albacore.calculus.CalculatorContext.StockingContext;
import net.butfly.albacore.calculus.Functor.Stocking;
import net.butfly.albacore.calculus.Functor.Streaming;
import net.butfly.albacore.calculus.Functor.Type;
import net.butfly.albacore.calculus.datasource.DataSource;

public class FunctorConfig implements Serializable {
	private static final long serialVersionUID = 5323846657146326084L;
	public Class<? extends Functor<?>> functorClass;
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

	static <F extends Functor<F>> FunctorConfig parse(Class<F> functor, Mode mode) throws IOException {
		if (null == functor) return null;
		FunctorConfig config = new FunctorConfig();
		config.functorClass = functor;
		switch (mode) {
		case STOCKING:
			config.stockingDSs.put(functor.getAnnotation(Stocking.class).source(), config.parseStockingConfig());
			break;
		case STREAMING:
			config.stockingDSs.put(functor.getAnnotation(Stocking.class).source(), config.parseStockingConfig());
			Streaming streaming = functor.getAnnotation(Streaming.class);
			if (streaming != null) config.streamingDSs.put(streaming.source(), config.parseStreamingConfig());
			break;
		}
		return config;
	}

	private <F extends Functor<F>> Detail parseStockingConfig() {
		Stocking stocking = functorClass.getAnnotation(Stocking.class);
		if (Functor.NOT_DEFINED.equals(stocking.source())) return null;
		Detail detail = null;
		switch (stocking.type()) {
		case HBASE:
			if (Functor.NOT_DEFINED.equals(stocking.table()))
				throw new IllegalArgumentException("Table not defined for functor " + functorClass.toString());
			detail = new Detail(stocking.table());
			break;
		case MONGODB:
			if (Functor.NOT_DEFINED.equals(stocking.table()))
				throw new IllegalArgumentException("Table not defined for functor " + functorClass.toString());
			detail = new Detail(stocking.table(), Functor.NOT_DEFINED.equals(stocking.filter()) ? null : stocking.filter());
			break;
		case CONSTAND_TO_CONSOLE:
			break;
		default:
			throw new UnsupportedOperationException("Unsupportted stocking mode: " + stocking.type());
		}
		// if (validate) {}
		return detail;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	void confirm(StockingContext context) {
		DataSource ds = context.datasources.get(functorClass.getAnnotation(Stocking.class).source());
		for (Detail detail : this.stockingDSs.values())
			ds.getMarshaller().confirm((Class<? extends Functor>) functorClass, ds, detail);
	}

	private <F extends Functor<F>> Detail parseStreamingConfig() {
		Streaming streaming = functorClass.getAnnotation(Streaming.class);
		if (streaming == null) return null;
		switch (streaming.type()) {
		case KAFKA:
			return new Detail(streaming.topics());
		default:
			throw new UnsupportedOperationException("Unsupportted streaming mode: " + streaming.type());
		}
	}
}
