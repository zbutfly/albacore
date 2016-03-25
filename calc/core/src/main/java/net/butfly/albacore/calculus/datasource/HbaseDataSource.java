package net.butfly.albacore.calculus.datasource;

import java.io.IOException;
import java.lang.reflect.Field;
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
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.CaseFormat;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factor.Type;
import net.butfly.albacore.calculus.marshall.HbaseMarshaller;
import net.butfly.albacore.calculus.marshall.Marshaller;
import net.butfly.albacore.calculus.streaming.JavaBatchPairDStream;
import net.butfly.albacore.calculus.utils.Reflections;
import scala.Tuple2;

public class HbaseDataSource extends DataSource<ImmutableBytesWritable, Result, HbaseDataDetail> {
	private static final long serialVersionUID = 3367501286179801635L;
	String configFile;
	Connection hconn;

	public HbaseDataSource(String configFile, Marshaller<ImmutableBytesWritable, Result> marshaller) {
		super(Type.HBASE, null == marshaller ? new HbaseMarshaller() : marshaller);
		this.configFile = configFile;
		// XXX
		// this.hconn = ConnectionFactory.createConnection(conf)
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

	@SuppressWarnings("unchecked")
	@Override
	public <K, F extends Factor<F>> JavaPairRDD<K, F> stocking(JavaSparkContext sc, Class<F> factor, HbaseDataDetail detail) {
		Configuration hconf = HBaseConfiguration.create();
		try {
			hconf.addResource(Calculator.scanInputStream(this.configFile));
		} catch (IOException e) {
			throw new RuntimeException("HBase configuration invalid.", e);
		}
		hconf.set(TableInputFormat.INPUT_TABLE, detail.hbaseTable);
		if (Calculator.debug) {
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
		// conf.hconf.set(TableInputFormat.SCAN_COLUMNS, "cf1:vc cf1:vs");

		JavaPairRDD<K, F> r = (JavaPairRDD<K, F>) sc
				.newAPIHadoopRDD(hconf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class).mapToPair(
						t -> null == t ? null : new Tuple2<>(this.marshaller.unmarshallId(t._1), this.marshaller.unmarshall(t._2, factor)));
		if (logger.isTraceEnabled()) logger.trace("Stocking from hbase: " + r.count());
		return r;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K, F extends Factor<F>> JavaPairDStream<K, F> batching(JavaStreamingContext ssc, Class<F> factor, long batching,
			HbaseDataDetail detail, Class<K> kClass, Class<F> vClass) {
		return new JavaBatchPairDStream<K, F>(ssc, (limit, offset) -> {
			Configuration hconf = HBaseConfiguration.create();
			try {
				hconf.addResource(Calculator.scanInputStream(configFile));
			} catch (IOException e) {
				throw new RuntimeException("HBase configuration invalid.", e);
			}
			hconf.set(TableInputFormat.INPUT_TABLE, detail.hbaseTable);

			Scan sc = new Scan();
			if (null != offset) sc = sc.setStartRow(marshaller.marshallId(offset.toString()).copyBytes());
			sc = sc.setFilter(new PageFilter(limit));
			String scstr = Base64.encodeBytes(ProtobufUtil.toScan(sc).toByteArray());
			hconf.set(TableInputFormat.SCAN, scstr);
			// conf.hconf.set(TableInputFormat.SCAN_COLUMNS, "cf1:vc
			// cf1:vs");
			JavaPairRDD<K, F> r = (JavaPairRDD<K, F>) ssc.sparkContext()
					.newAPIHadoopRDD(hconf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class)
					.mapToPair(t -> null == t ? null : new Tuple2<>(marshaller.unmarshallId(t._1), marshaller.unmarshall(t._2, factor)));
			if (logger.isTraceEnabled()) logger.trace("Batching from hbase: " + r.count());
			return r;
		} , batching, kClass, vClass);
	}
}