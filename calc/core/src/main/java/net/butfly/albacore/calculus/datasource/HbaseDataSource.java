package net.butfly.albacore.calculus.datasource;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.CaseFormat;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factor.Type;
import net.butfly.albacore.calculus.factor.filter.FactorFilter;
import net.butfly.albacore.calculus.lambda.Func;
import net.butfly.albacore.calculus.marshall.HbaseMarshaller;
import net.butfly.albacore.calculus.utils.Logable;
import net.butfly.albacore.calculus.utils.Reflections;
import scala.Tuple2;

@SuppressWarnings("rawtypes")
public class HbaseDataSource extends DataSource<byte[], ImmutableBytesWritable, Result, byte[], Mutation> {
	private static final long serialVersionUID = 3367501286179801635L;
	String configFile;

	public HbaseDataSource(String configFile, HbaseMarshaller marshaller) {
		super(Type.HBASE, false, null == marshaller ? new HbaseMarshaller() : marshaller, ImmutableBytesWritable.class, Result.class,
				TableOutputFormat.class);
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
	public <F> boolean confirm(Class<F> factor, DataDetail<F> detail) {
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
	public <F extends Factor<F>> JavaPairRDD<byte[], F> stocking(Calculator calc, Class<F> factor, DataDetail<F> detail,
			FactorFilter... filters) {
		debug(() -> "Scaning begin: " + factor.toString() + ", from table: " + detail.tables[0] + ".");
		Configuration conf = HBaseConfiguration.create();
		HUtil<F> util = new HUtil<F>(configFile, factor, detail.tables[0], (HbaseMarshaller) marshaller, calc.debug).init(conf);
		return util.filter(conf, util.filter(filters)).debug(conf).scan(calc.sc, conf);
	}

	@Override
	@Deprecated
	public <F extends Factor<F>> JavaPairRDD<byte[], F> batching(Calculator calc, Class<F> factor, long limit, byte[] offset,
			DataDetail<F> detail, FactorFilter... filters) {
		debug(() -> "Scaning begin: " + factor.toString() + ", from table: " + detail.tables[0] + ".");
		error(() -> "Batching mode is not supported now... BUG!!!!!");
		Configuration conf = HBaseConfiguration.create();
		HUtil<F> util = new HUtil<F>(configFile, factor, detail.tables[0], (HbaseMarshaller) marshaller, calc.debug).init(conf);
		return util.filter(conf, util.filter(new FactorFilter.Page<byte[]>(offset, limit))).scan(calc.sc, conf);
	}

	protected static class HUtil<F extends Factor<F>> implements Serializable, Logable {
		private static final long serialVersionUID = 2314819561624610201L;
		protected final static Logger logger = LoggerFactory.getLogger(HUtil.class);
		Class<F> factor;
		boolean filtered = false;
		boolean debug;
		private HbaseMarshaller marshaller;
		private String configFile;
		private String table;

		public HUtil(String configFile, Class<F> factor, String table, HbaseMarshaller marshaller, boolean debug) {
			super();
			this.configFile = configFile;
			this.table = table;
			this.factor = factor;
			this.debug = debug;
			this.marshaller = marshaller;
		}

		public HUtil<F> init(Configuration hconf) {
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
					.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, byte[], F>() {
						@Override
						public Tuple2<byte[], F> call(Tuple2<ImmutableBytesWritable, Result> t) throws Exception {
							return new Tuple2<>(marshaller.unmarshallId(t._1), marshaller.unmarshall(t._2, factor));
						}
					});
		}

		public HUtil<F> debug(final Configuration hconf) {
			if (debug && !filtered) try {
				float ratio = Float.parseFloat(System.getProperty("calculus.debug.hbase.random.ratio", "0"));
				if (ratio > 0) {
					error(() -> "Hbase debugging, random sampling results of " + ratio
							+ " (can be customized by -Dcalculus.debug.hbase.random.ratio=0.00000X)");
					hconf.set(TableInputFormat.SCAN,
							Base64.encodeBytes(ProtobufUtil.toScan(createScan().setFilter(new RandomRowFilter(ratio))).toByteArray()));
				} else {
					long limit = Long.parseLong(System.getProperty("calculus.debug.hbase.limit", "-1"));
					if (limit > 0) {
						error(() -> "Hbase debugging, limit results in " + limit
								+ " (can be customized by -Dcalculus.debug.hbase.limit=100)");
						hconf.set(TableInputFormat.SCAN,
								Base64.encodeBytes(ProtobufUtil.toScan(createScan().setFilter(new PageFilter(limit))).toByteArray()));
					}
				}
			} catch (IOException e) {
				error(() -> "Hbase debugging failure, page scan definition error", e);
			}
			filtered = true;
			return this;
		}

		public HUtil<F> filter(final Configuration hconf, Filter filter) {
			try {
				hconf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(createScan().setFilter(filter)).toByteArray()));
				filtered = true;
			} catch (IOException e) {
				throw new RuntimeException("HBase configuration invalid.", e);
			}
			return this;
		}

		private Filter filter(FactorFilter[] filters) {
			if (filters.length == 0) return null;
			if (filters.length == 1) return filter(filters[0]);
			FilterList fl = new FilterList(Operator.MUST_PASS_ALL);
			for (FactorFilter f : filters)
				if (f != null) fl.addFilter(filter(f));
			return fl;
		}

		private Filter filter(FactorFilter filter) {
			if (filter instanceof FactorFilter.ByField) {
				Field field = Reflections.getDeclaredField(factor, ((FactorFilter.ByField<?>) filter).field);
				String[] q = marshaller.parseField(field).split(":");
				byte[][] qulifiers = new byte[][] { Bytes.toBytes(q[0]), Bytes.toBytes(q[1]) };
				Func<Object, byte[]> conv = CONVERTERS.get(field.getType());
				if (filter instanceof FactorFilter.ByFieldValue) {
					if (!ops.containsKey(filter.getClass()))
						throw new UnsupportedOperationException("Unsupportted filter: " + filter.getClass());
					byte[] val = conv.call(((FactorFilter.ByFieldValue<?>) filter).value);
					SingleColumnValueFilter f = new SingleColumnValueFilter(qulifiers[0], qulifiers[1], ops.get(filter.getClass()), val);
					f.setFilterIfMissing(true);
					return f;
				}
				if (filter.getClass().equals(FactorFilter.In.class)) return new SingleColumnInValuesFilter(qulifiers[0], qulifiers[1],
						Reflections.transform(((FactorFilter.In<Object>) filter).values, new Func<Object, byte[]>() {
							@Override
							public byte[] call(Object v) {
								return conv.call(v);
							}
						}).toArray(new byte[0][]));
				if (filter.getClass().equals(FactorFilter.Regex.class)) {
					SingleColumnValueFilter f = new SingleColumnValueFilter(qulifiers[0], qulifiers[1], CompareOp.EQUAL,
							new RegexStringComparator(((FactorFilter.Regex) filter).regex.toString()));
					f.setFilterIfMissing(true);
					return f;
				}
			}
			if (filter.getClass().equals(FactorFilter.Limit.class)) return new PageFilter(((FactorFilter.Limit) filter).limit);
			// if (filter.getClass().equals(FactorFilter.Skip.class)) return new
			// SkipFilter(((FactorFilter.Skip) filter).skip);
			if (filter.getClass().equals(FactorFilter.And.class)) {
				FilterList ands = new FilterList(Operator.MUST_PASS_ALL);
				for (FactorFilter f : ((FactorFilter.And) filter).filters)
					ands.addFilter(filter(f));
				return ands;
			}
			if (filter.getClass().equals(FactorFilter.Or.class)) {
				FilterList ors = new FilterList(Operator.MUST_PASS_ONE);
				for (FactorFilter f : ((FactorFilter.And) filter).filters)
					ors.addFilter(filter(f));
				return ors;
			}

			throw new UnsupportedOperationException("Unsupportted filter: " + filter.getClass());
		}

		private static final Map<Class<? extends FactorFilter>, CompareOp> ops = new HashMap<>();
		static {
			ops.put(FactorFilter.Equal.class, CompareOp.EQUAL);
			ops.put(FactorFilter.NotEqual.class, CompareOp.NOT_EQUAL);
			ops.put(FactorFilter.Less.class, CompareOp.LESS);
			ops.put(FactorFilter.Greater.class, CompareOp.GREATER);
			ops.put(FactorFilter.LessOrEqual.class, CompareOp.LESS_OR_EQUAL);
			ops.put(FactorFilter.GreaterOrEqual.class, CompareOp.GREATER_OR_EQUAL);
		}

	}

	private static final Map<Class, Func<Object, byte[]>> CONVERTERS = new HashMap<>();

	static {
		CONVERTERS.put(String.class, new Func<Object, byte[]>() {
			@Override
			public byte[] call(Object val) {
				return null == val ? null : Bytes.toBytes((String) val);
			}
		});
		CONVERTERS.put(Integer.class, new Func<Object, byte[]>() {
			@Override
			public byte[] call(Object val) {
				return null == val ? null : Bytes.toBytes((Integer) val);
			}
		});
		CONVERTERS.put(Boolean.class, new Func<Object, byte[]>() {
			@Override
			public byte[] call(Object val) {
				return null == val ? null : Bytes.toBytes((Boolean) val);
			}
		});
		CONVERTERS.put(Long.class, new Func<Object, byte[]>() {
			@Override
			public byte[] call(Object val) {
				return null == val ? null : Bytes.toBytes((Long) val);
			}
		});
		CONVERTERS.put(Double.class, new Func<Object, byte[]>() {
			@Override
			public byte[] call(Object val) {
				return null == val ? null : Bytes.toBytes((Double) val);
			}
		});
		CONVERTERS.put(Float.class, new Func<Object, byte[]>() {
			@Override
			public byte[] call(Object val) {
				return null == val ? null : Bytes.toBytes((Float) val);
			}
		});
		CONVERTERS.put(Short.class, new Func<Object, byte[]>() {
			@Override
			public byte[] call(Object val) {
				return null == val ? null : Bytes.toBytes((Short) val);
			}
		});
		CONVERTERS.put(Byte.class, new Func<Object, byte[]>() {
			@Override
			public byte[] call(Object val) {
				return null == val ? null : Bytes.toBytes((Byte) val);
			}
		});
		CONVERTERS.put(BigDecimal.class, new Func<Object, byte[]>() {
			@Override
			public byte[] call(Object val) {
				return null == val ? null : Bytes.toBytes((BigDecimal) val);
			}
		});
	}

	private static Scan createScan() {
		Scan sc = new Scan();
		try {
			sc.setCaching(-1);
			sc.setCacheBlocks(false);
			// sc.setSmall(true);
		} catch (Throwable th) {
			// XXX
			try {
				sc.getClass().getMethod("setCacheBlocks", boolean.class).invoke(sc, false);
				// sc.getClass().getMethod("setSmall", boolean.class).invoke(sc,
				// false);
				sc.getClass().getMethod("setCaching", int.class).invoke(sc, -1);
			} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
					| SecurityException e) {}
		}
		return sc;
	}
}