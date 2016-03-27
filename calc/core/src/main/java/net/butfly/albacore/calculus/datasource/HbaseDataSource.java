package net.butfly.albacore.calculus.datasource;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.CaseFormat;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factor.Type;
import net.butfly.albacore.calculus.marshall.HbaseMarshaller;
import net.butfly.albacore.calculus.streaming.JavaBatchPairDStream;
import net.butfly.albacore.calculus.utils.Reflections;
import scala.Tuple2;

public class HbaseDataSource extends DataSource<String, ImmutableBytesWritable, Result, HbaseDataDetail> {
	private static final long serialVersionUID = 3367501286179801635L;
	String configFile;
	Connection hconn;

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

	public Connection getHconn() {
		return hconn;
	}

	@Override
	public boolean confirm(Class<? extends Factor<?>> factor, HbaseDataDetail detail) {
		try {
			TableName ht = TableName.valueOf(detail.hbaseTable);
			Admin a = getHconn().getAdmin();
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
			logger.error("Failure confirm data source: " + factor.getName() + " => " + this.toString() + " => " + detail.toString());
			return false;
		}
	}

	@Override
	public <F extends Factor<F>> JavaPairRDD<String, F> stocking(JavaSparkContext sc, Class<F> factor, HbaseDataDetail detail) {
		return this.scan(sc, detail.hbaseTable, factor, null);
	}

	@Override
	public <F extends Factor<F>> JavaPairDStream<String, F> batching(JavaStreamingContext ssc, Class<F> factor, long batching,
			HbaseDataDetail detail, Class<String> kClass, Class<F> vClass) {
		Function2<Long, String, JavaPairRDD<String, F>> batcher = (limit, offset) -> {
			Scan scan = createScan().setFilter(new PageFilter(limit));
			if (null != scan) scan.setStartRow(marshaller.marshallId((String) offset).copyBytes());
			return scan(ssc.sparkContext(), detail.hbaseTable, factor, scan);
		};
		return new JavaBatchPairDStream<String, F>(ssc, batcher, batching, kClass, vClass);
	}

	private <F extends Factor<F>> JavaPairRDD<String, F> scan(JavaSparkContext sc, String table, Class<F> factor, Scan scan) {
		Configuration hconf = HBaseConfiguration.create();
		try {
			hconf.addResource(Calculator.scanInputStream(this.configFile));
		} catch (IOException e) {
			throw new RuntimeException("HBase configuration invalid.", e);
		}
		hconf.set(TableInputFormat.INPUT_TABLE, table);
		if (null != scan) try {
			hconf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray()));
		} catch (IOException e) {
			throw new RuntimeException("HBase scan generating failure", e);
		}
		else if (Calculator.debug) {
			float ratio = Float.parseFloat(System.getProperty("calculus.debug.hbase.random.ratio", "0.01"));
			logger.error("Hbase debugging, random sampling results of " + (ratio * 100)
					+ "% (can be customized by -Dcalculus.debug.hbase.random.ratio=" + ratio + ")");
			try {
				hconf.set(TableInputFormat.SCAN,
						Base64.encodeBytes(ProtobufUtil.toScan(new Scan().setFilter(new RandomRowFilter(ratio))).toByteArray()));
			} catch (IOException e) {
				logger.error("Hbase debugging failure, page scan definition error", e);
			}
		}
		JavaPairRDD<ImmutableBytesWritable, Result> rr = sc.newAPIHadoopRDD(hconf, TableInputFormat.class, ImmutableBytesWritable.class,
				Result.class);
		if (logger.isTraceEnabled()) logger.trace("HBase scaned: " + rr.count());
		// TODO: Confirm String key
		return rr.mapToPair(t -> null == t ? null
				: new Tuple2<String, F>(this.marshaller.unmarshallId(t._1), this.marshaller.unmarshall(t._2, factor)));
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