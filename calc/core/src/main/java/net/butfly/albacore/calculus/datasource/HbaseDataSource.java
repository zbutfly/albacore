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
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.CaseFormat;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factor.Type;
import net.butfly.albacore.calculus.factor.filter.FactorFilter;
import net.butfly.albacore.calculus.marshall.HbaseMarshaller;
import net.butfly.albacore.calculus.utils.Reflections;

public class HbaseDataSource extends DataSource<byte[], ImmutableBytesWritable, Result, byte[], Mutation> {
	private static final long serialVersionUID = 3367501286179801635L;
	final String configFile;

	public HbaseDataSource(String configFile, HbaseMarshaller marshaller) {
		super(Type.HBASE, false, null == marshaller ? new HbaseMarshaller() : marshaller, ImmutableBytesWritable.class, Result.class,
				TableOutputFormat.class, TableInputFormat.class);
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
			float expandPartitions, FactorFilter... filters) {
		if (calc.debug && debugLimit > 0 && debugRandomChance > 0) filters = adddebug(filters);
		debug(() -> "Scaning begin: " + factor.toString() + ", from table: " + detail.tables[0] + ".");
		Configuration conf = HBaseConfiguration.create();
		HbaseConfiguration<F> util = new HbaseConfiguration<F>(this.configFile, (HbaseMarshaller) this.marshaller, factor, detail.tables[0])
				.init(conf);
		return read(calc.sc, util.filter(conf, filters), factor, expandPartitions);
	}

	@Override
	@Deprecated
	public <F extends Factor<F>> JavaPairRDD<byte[], F> batching(Calculator calc, Class<F> factor, long limit, byte[] offset,
			DataDetail<F> detail, FactorFilter... filters) {
		debug(() -> "Scaning begin: " + factor.toString() + ", from table: " + detail.tables[0] + ".");
		error(() -> "Batching mode is not supported now... BUG!!!!!");
		Configuration conf = HBaseConfiguration.create();
		HbaseConfiguration<F> util = new HbaseConfiguration<F>(this.configFile, (HbaseMarshaller) this.marshaller, factor, detail.tables[0])
				.init(conf);
		return read(calc.sc, util.filter(conf, new FactorFilter.Page<byte[]>(offset, limit)), factor, -1);
	}
}