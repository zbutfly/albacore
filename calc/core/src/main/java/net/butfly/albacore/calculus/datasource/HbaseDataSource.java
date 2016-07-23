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
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Base64;

import com.google.common.base.CaseFormat;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factor.Type;
import net.butfly.albacore.calculus.factor.filter.FactorFilter;
import net.butfly.albacore.calculus.factor.filter.HbaseBuilder;
import net.butfly.albacore.calculus.factor.rds.PairRDS;
import net.butfly.albacore.calculus.factor.rds.internal.PairWrapped;
import net.butfly.albacore.calculus.marshall.HbaseMarshaller;
import net.butfly.albacore.calculus.utils.Reflections;

public class HbaseDataSource extends DataSource<byte[], ImmutableBytesWritable, Result, byte[], Mutation> {
	private static final long serialVersionUID = 3367501286179801635L;
	final String configFile;

	public HbaseDataSource(String configFile, CaseFormat srcf, CaseFormat dstf) {
		super(Type.HBASE, false, HbaseMarshaller.class, ImmutableBytesWritable.class, Result.class, TableOutputFormat.class,
				TableInputFormat.class, srcf, dstf);
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
				String colname = marshaller.parseQualifier(f);
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
	public <F extends Factor<F>> PairRDS<byte[], F> stocking(Calculator calc, Class<F> factor, DataDetail<F> detail, float expandPartitions,
			FactorFilter... filters) {
		if (calc.debug) filters = enableDebug(filters);
		debug(() -> "Scaning begin: " + factor.toString() + " from table: " + detail.tables[0] + ".");
		return readByInputFormat(calc.sc,
				addHbaseFilter(new HbaseBuilder<F>(factor, (HbaseMarshaller) this.marshaller), createHbaseConf(detail.tables[0]), filters),
				factor, expandPartitions);
	}

	@Override
	@Deprecated
	public <F extends Factor<F>> PairWrapped<byte[], F> batching(Calculator calc, Class<F> factor, long limit, byte[] offset,
			DataDetail<F> detail, FactorFilter... filters) {
		debug(() -> "Scaning begin: " + factor.toString() + " from table: " + detail.tables[0] + ".");
		error(() -> "Batching mode is not supported now... BUG!!!!!");
		return readByInputFormat(calc.sc, addHbaseFilter(new HbaseBuilder<F>(factor, (HbaseMarshaller) this.marshaller),
				createHbaseConf(detail.tables[0]), new FactorFilter.Page<byte[]>(offset, limit)), factor, -1);
	}

	private Configuration createHbaseConf(String table) {
		Configuration hconf = HBaseConfiguration.create();
		try {
			hconf.addResource(Calculator.scanInputStream(configFile));
		} catch (IOException e) {
			throw new RuntimeException("HBase configuration invalid.", e);
		}
		hconf.set(TableInputFormat.INPUT_TABLE, table);
		return hconf;
	}

	private Configuration addHbaseFilter(HbaseBuilder<?> builder, Configuration conf, FactorFilter... filters) {
		if (filters.length == 0) return conf;
		Scan sc = new Scan();
		try {
			sc.setCaching(-1);
			sc.setCacheBlocks(false);
			// sc.setSmall(true);
		} catch (Throwable th) {
			// XXX
			try {
				sc.getClass().getMethod("setCacheBlocks", boolean.class).invoke(sc, false);
				// sc.getClass().getMethod("setSmall",
				// boolean.class).invoke(sc,
				// false);
				sc.getClass().getMethod("setCaching", int.class).invoke(sc, -1);
			} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
					| SecurityException e) {}
		}
		try {
			Filter f = builder.filter(filters);
			if (null != f) conf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(sc.setFilter(f)).toByteArray()));
			return conf;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}