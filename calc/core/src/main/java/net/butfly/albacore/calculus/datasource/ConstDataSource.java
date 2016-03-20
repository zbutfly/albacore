package net.butfly.albacore.calculus.datasource;

import java.util.Arrays;
import java.util.UUID;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import com.google.common.base.Joiner;

import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factor.Type;
import net.butfly.albacore.calculus.utils.Reflections;
import scala.Tuple2;

public class ConstDataSource extends DataSource<Void, Void, Detail> {
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

	@SuppressWarnings("unchecked")
	@Override
	public <K, F extends Factor<F>> JavaPairRDD<K, F> stocking(JavaSparkContext sc, Class<F> factor, Detail detail) {
		String[] values = this.values;
		if (values == null) values = new String[0];
		return (JavaPairRDD<K, F>) sc.parallelize(Arrays.asList(values))
				.mapToPair(t -> null == t ? null : new Tuple2<>(UUID.randomUUID().toString(), (F) Reflections.construct(factor, t)));
	}

	@Override
	public <K, F extends Factor<F>> VoidFunction<JavaPairRDD<K, F>> saving(JavaSparkContext sc, Detail detail) {
		if (values == null) values = new String[0];
		return r -> {
			if (null != r) for (Tuple2<?, F> o : r.collect())
				logger.info("Calculated, result => " + o._2.toString());
		};
	}
}