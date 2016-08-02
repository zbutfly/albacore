package net.butfly.albacore.calculus.factor.filter;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.lambda.Func;
import net.butfly.albacore.calculus.marshall.HbaseMarshaller;
import net.butfly.albacore.calculus.utils.Reflections;

public class HbaseBuilder<F extends Factor<F>> extends Builder<Filter, byte[], F> {
	private static final long serialVersionUID = 8586300137478563435L;

	public HbaseBuilder(Class<F> factor, HbaseMarshaller marshaller) {
		super(factor, marshaller);
	}

	@Override
	public Filter filter(FactorFilter... filter) {
		if (filter.length == 0) return null;
		if (filter.length == 1) return filterOne(filter[0]);
		FilterList fl = new FilterList(Operator.MUST_PASS_ALL);
		for (FactorFilter f : filter) {
			Filter q = filterOne(f);
			if (null != q) fl.addFilter(q);
		}
		return fl;
	}

	@Override
	protected byte[] expression(Class<?> type, Object value) {
		return CONVERTERS.get(type).call(value);
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected Filter filterOne(FactorFilter filter) {
		if (null == filter) return null;
		if (filter instanceof FactorFilter.ByField) {
			Field field = Reflections.getDeclaredField(factor, ((FactorFilter.ByField<?>) filter).field);
			String[] q = marshaller.parseQualifier(field).split(":");
			byte[][] qulifiers = new byte[][] { Bytes.toBytes(q[0]), Bytes.toBytes(q[1]) };
			if (filter instanceof FactorFilter.ByFieldValue) {
				if (!ops.containsKey(filter.getClass())) throw new UnsupportedOperationException("Unsupportted filter: " + filter
						.getClass());
				SingleColumnValueFilter f = new SingleColumnValueFilter(qulifiers[0], qulifiers[1], ops.get(filter.getClass()), expression(
						field.getType(), ((FactorFilter.ByFieldValue<?>) filter).value));
				f.setFilterIfMissing(true);
				return f;
			}
			if (filter.getClass().equals(FactorFilter.In.class) && ((FactorFilter.In) filter).values.size() > 0) {
				FilterList fl = new FilterList(Operator.MUST_PASS_ONE);
				for (Object v : ((FactorFilter.In) filter).values)
					fl.addFilter(new SingleColumnValueFilter(qulifiers[0], qulifiers[1], CompareOp.EQUAL, expression(field.getType(), v)));
				return fl;
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
		if (filter.getClass().equals(FactorFilter.And.class) && ((FactorFilter.And) filter).filters.size() > 0) {
			FilterList ands = new FilterList(Operator.MUST_PASS_ALL);
			for (FactorFilter f : ((FactorFilter.And) filter).filters)
				ands.addFilter(filter(f));
			return ands;
		}
		if (filter.getClass().equals(FactorFilter.Or.class) && ((FactorFilter.Or) filter).filters.size() > 0) {
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

		CONVERTERS.put(int.class, CONVERTERS.get(Integer.class));
		CONVERTERS.put(boolean.class, CONVERTERS.get(Boolean.class));
		CONVERTERS.put(long.class, CONVERTERS.get(Long.class));
		CONVERTERS.put(double.class, CONVERTERS.get(Double.class));
		CONVERTERS.put(float.class, CONVERTERS.get(Float.class));
		CONVERTERS.put(short.class, CONVERTERS.get(Short.class));
		CONVERTERS.put(byte.class, CONVERTERS.get(Byte.class));
	}
}
