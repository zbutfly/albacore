package net.butfly.albacore.calculus.datasource;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.filter.FactorFilter;
import net.butfly.albacore.calculus.lambda.Func;
import net.butfly.albacore.calculus.marshall.HbaseMarshaller;
import net.butfly.albacore.calculus.utils.Logable;
import net.butfly.albacore.calculus.utils.Reflections;

class HbaseConfiguration<F extends Factor<F>> implements Serializable, Logable {
	private static final long serialVersionUID = 2314819561624610201L;
	protected final static Logger logger = LoggerFactory.getLogger(HbaseConfiguration.class);
	private final String table;
	private final Class<F> factor;
	private final String configFile;
	private final HbaseMarshaller marshaller;

	public HbaseConfiguration(String configFile, HbaseMarshaller marshaller, Class<F> factor, String table) {
		super();
		this.configFile = configFile;
		this.marshaller = marshaller;
		this.table = table;
		this.factor = factor;
	}

	public HbaseConfiguration<F> init(Configuration hconf) {
		try {
			hconf.addResource(Calculator.scanInputStream(configFile));
		} catch (IOException e) {
			throw new RuntimeException("HBase configuration invalid.", e);
		}
		hconf.set(TableInputFormat.INPUT_TABLE, table);
		return this;
	}

	public Configuration filter(Configuration conf, FactorFilter... filters) {
		if (filters.length == 0) return conf;
		try {
			if (filters.length > 1) {
				FilterList fl = new FilterList(Operator.MUST_PASS_ALL);
				for (FactorFilter f : filters)
					if (f != null) fl.addFilter(filterOne(f));
				conf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(createScan().setFilter(fl)).toByteArray()));
				return conf;
			}
			conf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(createScan().setFilter(filterOne(filters[0])))
					.toByteArray()));
			return conf;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	private Filter filterOne(FactorFilter filter) {
		if (filter instanceof FactorFilter.ByField) {
			Field field = Reflections.getDeclaredField(factor, ((FactorFilter.ByField<?>) filter).field);
			String[] q = marshaller.parseField(field).split(":");
			byte[][] qulifiers = new byte[][] { Bytes.toBytes(q[0]), Bytes.toBytes(q[1]) };
			Func<Object, byte[]> conv = CONVERTERS.get(field.getType());
			if (filter instanceof FactorFilter.ByFieldValue) {
				if (!ops.containsKey(filter.getClass())) throw new UnsupportedOperationException("Unsupportted filter: " + filter
						.getClass());
				Object value = ((FactorFilter.ByFieldValue<?>) filter).value;
				byte[] val = conv.call(value);
				SingleColumnValueFilter f = new SingleColumnValueFilter(qulifiers[0], qulifiers[1], ops.get(filter.getClass()), val);
				f.setFilterIfMissing(true);
				return f;
			}
			if (filter.getClass().equals(FactorFilter.In.class)) {
				@SuppressWarnings("rawtypes")
				Collection<Object> values = ((FactorFilter.In) filter).values;
				return new SingleColumnInValuesFilter(qulifiers[0], qulifiers[1], Reflections.transform(values, conv::call).toArray(
						new byte[0][]));
			}
			if (filter.getClass().equals(FactorFilter.Regex.class)) {
				SingleColumnValueFilter f = new SingleColumnValueFilter(qulifiers[0], qulifiers[1], CompareOp.EQUAL,
						new RegexStringComparator(((FactorFilter.Regex) filter).regex.toString()));
				f.setFilterIfMissing(true);
				return f;
			}
		}
		if (filter.getClass().equals(FactorFilter.Limit.class)) return new PageFilter(((FactorFilter.Limit) filter).limit);
		if (filter.getClass().equals(FactorFilter.Random.class)) return new RandomRowFilter(((FactorFilter.Random) filter).chance);
		if (filter.getClass().equals(FactorFilter.And.class)) {
			FilterList ands = new FilterList(Operator.MUST_PASS_ALL);
			for (FactorFilter f : ((FactorFilter.And) filter).filters)
				ands.addFilter(filterOne(f));
			return ands;
		}
		if (filter.getClass().equals(FactorFilter.Or.class)) {
			FilterList ors = new FilterList(Operator.MUST_PASS_ONE);
			for (FactorFilter f : ((FactorFilter.And) filter).filters)
				ors.addFilter(filterOne(f));
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

	@SuppressWarnings("rawtypes")
	private static final Map<Class, Func<Object, byte[]>> CONVERTERS = new HashMap<>();

	static {
		CONVERTERS.put(String.class, val -> null == val ? null : Bytes.toBytes((String) val));
		CONVERTERS.put(Integer.class, val -> null == val ? null : Bytes.toBytes((Integer) val));
		CONVERTERS.put(Boolean.class, val -> null == val ? null : Bytes.toBytes((Boolean) val));
		CONVERTERS.put(Long.class, val -> null == val ? null : Bytes.toBytes((Long) val));
		CONVERTERS.put(Double.class, val -> null == val ? null : Bytes.toBytes((Double) val));
		CONVERTERS.put(Float.class, val -> null == val ? null : Bytes.toBytes((Float) val));
		CONVERTERS.put(Short.class, val -> null == val ? null : Bytes.toBytes((Short) val));
		CONVERTERS.put(Byte.class, val -> null == val ? null : Bytes.toBytes((Byte) val));
		CONVERTERS.put(BigDecimal.class, val -> null == val ? null : Bytes.toBytes((BigDecimal) val));
	}

}