package net.butfly.albacore.calculus.datasource;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.CaseFormat;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factor.Type;
import net.butfly.albacore.calculus.factor.filter.Filter.FieldFilter;
import net.butfly.albacore.calculus.factor.filter.Filter.In;
import net.butfly.albacore.calculus.marshall.HbaseMarshaller;
import net.butfly.albacore.calculus.utils.Reflections;
import scala.Tuple2;

public class HbaseDataSource extends DataSource<byte[], ImmutableBytesWritable, Result, HbaseDataDetail> {
	private static final long serialVersionUID = 3367501286179801635L;
	String configFile;

	public HbaseDataSource(String configFile, HbaseMarshaller marshaller) {
		super(Type.HBASE, false, null == marshaller ? new HbaseMarshaller() : marshaller);
		this.configFile = configFile;
	}

	@Override
	public String toString() {
		return super.toString() + ":" + this.configFile;
	}

	public String getConfigFile() {
		return configFile;
	}

	@Override
	public boolean confirm(Class<? extends Factor<?>> factor, HbaseDataDetail detail) {
		try {
			TableName ht = TableName.valueOf(detail.tables[0]);
			Configuration hconf = HBaseConfiguration.create();
			hconf.addResource(Calculator.scanInputStream(configFile));
			Admin a = ConnectionFactory.createConnection(hconf).getAdmin();
			if (a.tableExists(ht)) return true;
			Set<String> families = new HashSet<>();
			Set<String> columns = new HashSet<>();
			String dcf = factor.isAnnotationPresent(HbaseColumnFamily.class) ? factor.getAnnotation(HbaseColumnFamily.class).value() : null;
			families.add(dcf);
			for (Field f : Reflections.getDeclaredFields(factor)) {
				String colname = f.isAnnotationPresent(JsonProperty.class) ? f.getAnnotation(JsonProperty.class).value()
						: CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, f.getName());
				String colfamily = f.isAnnotationPresent(HbaseColumnFamily.class) ? f.getAnnotation(HbaseColumnFamily.class).value() : dcf;
				families.add(colfamily);
				columns.add(colfamily + ":" + colname);
			}
			HTableDescriptor td = new HTableDescriptor(ht);
			for (String fn : families) {
				HColumnDescriptor fd = new HColumnDescriptor(fn);
				td.addFamily(fd);
			}
			a.createTable(td);
			a.disableTable(ht);
			for (String col : columns)
				a.addColumn(ht, new HColumnDescriptor(col));
			a.enableTable(ht);
			return true;
		} catch (IOException e) {
			error(() -> "Failure confirm rddsOrDStream source: " + factor.getName() + " => " + this.toString() + " => " + detail.toString(),
					e);
			return false;
		}
	}

	@Override
	public <F extends Factor<F>> JavaPairRDD<byte[], F> stocking(Calculator calc, Class<F> factor, HbaseDataDetail detail,
			net.butfly.albacore.calculus.factor.filter.Filter... filters) {
		debug(() -> "Scaning begin: " + factor.toString() + ", from table: " + detail.tables[0] + ".");
		Configuration conf = HBaseConfiguration.create();
		return new HConf<F>(configFile, factor, detail.tables[0], (HbaseMarshaller) marshaller, calc.debug).init(conf).filter(conf, filters)
				.debug(conf).scan(calc.sc, conf);
	}

	@Override
	@Deprecated
	public <F extends Factor<F>> JavaPairRDD<byte[], F> batching(Calculator calc, Class<F> factor, long limit, byte[] offset,
			HbaseDataDetail detail) {
		debug(() -> "Scaning begin: " + factor.toString() + ", from table: " + detail.tables[0] + ".");
		error(() -> "Batching mode is not supported now... BUG!!!!!");
		Configuration conf = HBaseConfiguration.create();
		return new HConf<F>(configFile, factor, detail.tables[0], (HbaseMarshaller) marshaller, calc.debug).init(conf)
				.filter(conf, new net.butfly.albacore.calculus.factor.filter.Filter.Page<byte[]>(offset, limit)).scan(calc.sc, conf);
	}

	private static class HConf<F extends Factor<F>> implements Serializable {
		private static final long serialVersionUID = 2314819561624610201L;
		private final static Logger logger = LoggerFactory.getLogger(HConf.class);
		Class<F> factor;
		boolean filtered = false;
		boolean debug;
		private HbaseMarshaller marshaller;
		private String configFile;
		private String table;

		public HConf(String configFile, Class<F> factor, String table, HbaseMarshaller marshaller, boolean debug) {
			super();
			this.configFile = configFile;
			this.table = table;
			this.factor = factor;
			this.debug = debug;
			this.marshaller = marshaller;
		}

		public HConf<F> init(Configuration hconf) {
			try {
				hconf.addResource(Calculator.scanInputStream(configFile));
			} catch (IOException e) {
				throw new RuntimeException("HBase configuration invalid.", e);
			}
			hconf.set(TableInputFormat.INPUT_TABLE, table);
			return this;
		}

		public JavaPairRDD<byte[], F> scan(JavaSparkContext sc, Configuration hconf) {
			return sc.newAPIHadoopRDD(hconf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class)
					.mapToPair(t -> new Tuple2<>(marshaller.unmarshallId(t._1), marshaller.unmarshall(t._2, factor)));
		}

		public HConf<F> debug(final Configuration hconf) {
			if (debug && !filtered) try {
				float ratio = Float.parseFloat(System.getProperty("calculus.debug.hbase.random.ratio", "0"));
				if (ratio > 0) {
					logger.error("Hbase debugging, random sampling results of " + ratio
							+ " (can be customized by -Dcalculus.debug.hbase.random.ratio=0.00000X)");
					hconf.set(TableInputFormat.SCAN,
							Base64.encodeBytes(ProtobufUtil.toScan(createScan().setFilter(new RandomRowFilter(ratio))).toByteArray()));
				} else {
					long limit = Long.parseLong(System.getProperty("calculus.debug.hbase.limit", "-1"));
					if (limit > 0) {
						logger.error(
								"Hbase debugging, limit results in " + limit + " (can be customized by -Dcalculus.debug.hbase.limit=100)");
						hconf.set(TableInputFormat.SCAN,
								Base64.encodeBytes(ProtobufUtil.toScan(createScan().setFilter(new PageFilter(limit))).toByteArray()));
					}
				}
			} catch (IOException e) {
				logger.error("Hbase debugging failure, page scan definition error", e);
			}
			filtered = true;
			return this;
		}

		@SuppressWarnings("unchecked")
		public HConf<F> filter(Configuration hconf, net.butfly.albacore.calculus.factor.filter.Filter... filters) {
			if (filters.length > 0) {
				filtered = true;
				FilterList fl = new FilterList(Operator.MUST_PASS_ALL);
				for (net.butfly.albacore.calculus.factor.filter.Filter f : filters) {
					if (f instanceof FieldFilter) {
						Field field = Reflections.getDeclaredField(factor, ((FieldFilter<?>) f).field);
						String[] qulifier = marshaller.parseField(field).split(":");
						if (f.getClass().equals(In.class)) for (Filter hf : in(qulifier[0], qulifier[1], CONVERTERS.get(field.getType()),
								(Collection<Object>) ((In<?>) f).values))
							fl.addFilter(hf);
					}
				}
				try {
					hconf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(createScan().setFilter(fl)).toByteArray()));
				} catch (IOException e) {
					throw new RuntimeException("HBase configuration invalid.", e);
				}
			}
			return this;
		}

		public List<Filter> in(String family, String column, Function<Object, byte[]> conv, Collection<Object> referValues) {
			return new ArrayList<>(Reflections.transform(referValues, val -> new SingleColumnValueFilter(Bytes.toBytes(family),
					Bytes.toBytes(column), CompareOp.EQUAL, null == val ? null : conv.apply(val))));
		}

		private Scan createScan() {
			Scan sc = new Scan();
			try {
				sc.setCaching(-1);
				sc.setCacheBlocks(false);
				sc.setSmall(true);
			} catch (Throwable th) {
				// XXX
				try {
					sc.getClass().getMethod("setCacheBlocks", boolean.class).invoke(sc, false);
					sc.getClass().getMethod("setSmall", boolean.class).invoke(sc, false);
					sc.getClass().getMethod("setCaching", int.class).invoke(sc, -1);
				} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
						| SecurityException e) {}
			}
			return sc;
		}
	}

	@SuppressWarnings("rawtypes")
	private static final Map<Class, Function<Object, byte[]>> CONVERTERS = new HashMap<>();

	static {
		CONVERTERS.put(String.class, val -> Bytes.toBytes((String) val));
		CONVERTERS.put(Integer.class, val -> Bytes.toBytes((Integer) val));
		CONVERTERS.put(Boolean.class, val -> Bytes.toBytes((Boolean) val));
		CONVERTERS.put(Long.class, val -> Bytes.toBytes((Long) val));
		CONVERTERS.put(Double.class, val -> Bytes.toBytes((Double) val));
		CONVERTERS.put(Float.class, val -> Bytes.toBytes((Float) val));
		CONVERTERS.put(Short.class, val -> Bytes.toBytes((Short) val));
		CONVERTERS.put(Byte.class, val -> Bytes.toBytes((Byte) val));
		CONVERTERS.put(BigDecimal.class, val -> Bytes.toBytes((BigDecimal) val));
	}
}