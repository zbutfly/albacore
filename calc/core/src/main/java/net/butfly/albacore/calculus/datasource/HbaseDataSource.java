package net.butfly.albacore.calculus.datasource;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factor.Type;
import net.butfly.albacore.calculus.marshall.HbaseMarshaller;
import net.butfly.albacore.calculus.utils.Reflections;
import scala.Tuple2;

public class HbaseDataSource extends DataSource<byte[], ImmutableBytesWritable, Result, HbaseDataDetail> {
	private static final long serialVersionUID = 3367501286179801635L;
	String configFile;

	public HbaseDataSource(String configFile, HbaseMarshaller marshaller) {
		super(Type.HBASE, null == marshaller ? new HbaseMarshaller() : marshaller);
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
			logger.error("Failure confirm rddsOrDStream source: " + factor.getName() + " => " + this.toString() + " => " + detail
					.toString());
			return false;
		}
	}

	@Override
	public <F extends Factor<F>> JavaPairRDD<byte[], F> stocking(Calculator calc, Class<F> factor, HbaseDataDetail detail,
			String referField, Set<?> referValues) {
		if (logger.isDebugEnabled()) logger.debug("Scaning begin: " + factor.toString() + ", from table: " + detail.tables[0] + ".");
		return new HConf<F>(factor, detail.tables[0], calc.debug).filter(referField, referValues).debug().scan(calc.sc);
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

	@Override
	@Deprecated
	public <F extends Factor<F>> JavaPairRDD<byte[], F> batching(Calculator calc, Class<F> factor, long limit, byte[] offset,
			HbaseDataDetail detail) {
		if (logger.isDebugEnabled()) logger.debug("Scaning begin: " + factor.toString() + ", from table: " + detail.tables[0] + ".");
		logger.error("Batching mode is not supported now... BUG!!!!!");
		return new HConf<F>(factor, detail.tables[0], calc.debug).filter(offset, limit).scan(calc.sc);
	}

	private class HConf<F extends Factor<F>> {
		Configuration hconf;
		Class<F> factor;
		boolean filtered = false;
		boolean debug;

		public HConf(Class<F> factor, String table, boolean debug) {
			super();
			this.factor = factor;
			this.debug = debug;
			this.hconf = HBaseConfiguration.create();
			try {
				hconf.addResource(Calculator.scanInputStream(configFile));
			} catch (IOException e) {
				throw new RuntimeException("HBase configuration invalid.", e);
			}
			hconf.set(TableInputFormat.INPUT_TABLE, table);
		}

		public HConf<F> filter(byte[] offset, long limit) {
			try {
				hconf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(createScan().setFilter(new PageFilter(limit)))
						.toByteArray()));
			} catch (IOException e) {}
			if (null != offset) hconf.set("hbase.mapreduce.batching.offsets", Bytes.toString(offset));
			return this;
		}

		public JavaPairRDD<byte[], F> scan(JavaSparkContext sc) {
			JavaPairRDD<ImmutableBytesWritable, Result> rr = sc.newAPIHadoopRDD(hconf, TableInputFormat.class, ImmutableBytesWritable.class,
					Result.class);
			JavaPairRDD<byte[], F> r = rr.mapToPair(t -> null == t ? null
					: new Tuple2<byte[], F>(marshaller.unmarshallId(t._1), marshaller.unmarshall(t._2, factor)));
			if (debug && logger.isTraceEnabled()) logger.trace("HBase scaned: " + rr.count());
			return r;
		}

		public HConf<F> debug() {
			if (debug && !filtered) try {
				float ratio = Float.parseFloat(System.getProperty("calculus.debug.hbase.random.ratio", "0"));
				if (ratio > 0) {
					logger.error("Hbase debugging, random sampling results of " + ratio
							+ " (can be customized by -Dcalculus.debug.hbase.random.ratio=0.00000X)");
					hconf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(new Scan().setFilter(new RandomRowFilter(
							ratio))).toByteArray()));
				} else {
					long limit = Long.parseLong(System.getProperty("calculus.debug.hbase.limit", "-1"));
					if (limit > 0) {
						logger.error("Hbase debugging, limit results in " + limit
								+ " (can be customized by -Dcalculus.debug.hbase.limit=100)");
						hconf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(new Scan().setFilter(new PageFilter(limit)))
								.toByteArray()));
					}
				}
			} catch (IOException e) {
				logger.error("Hbase debugging failure, page scan definition error", e);
			}
			filtered = true;
			return this;
		}

		@SuppressWarnings("unchecked")
		public <V> HConf<F> filter(String referField, Set<V> referValues) {
			if (referField != null && referValues != null && referValues.size() > 0) {
				Field f = Reflections.getDeclaredField(factor, referField);
				String[] qulifier = ((HbaseMarshaller) marshaller).parseQulifier(factor, f);
				try {
					hconf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(new Scan().setFilter(new FilterList(
							Operator.MUST_PASS_ONE, Reflections.transform(Reflections.transform(referValues, converter((Class<V>) f
									.getType())), val -> new SingleColumnValueFilter(Bytes.toBytes(qulifier[0]), Bytes.toBytes(qulifier[1]),
											CompareOp.EQUAL, val)).toArray(new Filter[0])))).toByteArray()));
				} catch (IOException e) {
					throw new RuntimeException("HBase configuration invalid.", e);
				}
				filtered = true;
			}
			return this;
		}

		private <V> Function<V, byte[]> converter(Class<V> c) {
			if (null == c) return null;
			if (String.class.isAssignableFrom(c)) return val -> Bytes.toBytes((String) val);
			else if (Integer.class.isAssignableFrom(c)) return val -> Bytes.toBytes((Integer) val);
			else if (Boolean.class.isAssignableFrom(c)) return val -> Bytes.toBytes((Boolean) val);
			else if (Long.class.isAssignableFrom(c)) return val -> Bytes.toBytes((Long) val);
			else if (Double.class.isAssignableFrom(c)) return val -> Bytes.toBytes((Double) val);
			else if (Float.class.isAssignableFrom(c)) return val -> Bytes.toBytes((Float) val);
			else if (Short.class.isAssignableFrom(c)) return val -> Bytes.toBytes((Short) val);
			else if (Byte.class.isAssignableFrom(c)) return val -> Bytes.toBytes((Byte) val);
			else if (BigDecimal.class.isAssignableFrom(c)) return val -> Bytes.toBytes((BigDecimal) val);
			else throw new UnsupportedOperationException("Class " + c.toString() + " not supported.");
		}
	}
}