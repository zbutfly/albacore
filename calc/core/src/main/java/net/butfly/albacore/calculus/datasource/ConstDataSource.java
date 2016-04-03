package net.butfly.albacore.calculus.datasource;

import java.util.Arrays;
import java.util.Set;
import java.util.UUID;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;

import com.google.common.base.Joiner;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factor.Type;
import net.butfly.albacore.calculus.utils.Reflections;
import scala.Tuple2;

public class ConstDataSource extends DataSource<String, Void, Void, DataDetail> {
	private static final long serialVersionUID = -673387208224779163L;
	private String[] values;

	public ConstDataSource(String[] values) {
		super(Type.CONSTAND_TO_CONSOLE, null);
		this.values = values;
	}

	@Override
	public String toString() {
		return super.toString() + ":" + Joiner.on(',').join(values);
	}

	public String[] getValues() {
		return values;
	}

	@Override
	public <F extends Factor<F>> JavaPairRDD<String, F> stocking(Calculator calc, Class<F> factor, DataDetail detail, String referField,
			Set<?> referValues) {
		if (null != referField) throw new IllegalArgumentException("Constant data source does not support filter on reading.");
		String[] values = this.values;
		if (values == null) values = new String[0];
		return calc.sc.parallelize(Arrays.asList(values)).mapToPair(
				t -> null == t ? null : new Tuple2<String, F>(UUID.randomUUID().toString(), (F) Reflections.construct(factor, t)));
	}

	@Override
	public <F extends Factor<F>> VoidFunction<JavaPairRDD<String, F>> saving(Calculator calc, DataDetail detail) {
		if (values == null) values = new String[0];
		return r -> {
			if (null != r) for (Tuple2<?, F> o : r.collect())
				logger.info("Calculated, result => " + o._2.toString());
		};
	}
}