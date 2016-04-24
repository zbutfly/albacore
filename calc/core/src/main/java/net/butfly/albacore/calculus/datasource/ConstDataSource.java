package net.butfly.albacore.calculus.datasource;

import java.util.Arrays;
import java.util.UUID;

import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.google.common.base.Joiner;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factor.Type;
import net.butfly.albacore.calculus.factor.filter.FactorFilter;
import net.butfly.albacore.calculus.utils.Reflections;
import scala.Tuple2;

public class ConstDataSource extends DataSource<String, Void, String, Void, String> {
	private static final long serialVersionUID = -673387208224779163L;
	private String[] values;

	public ConstDataSource(String[] values) {
		super(Type.CONSTAND_TO_CONSOLE, false, null, Void.class, String.class, NullOutputFormat.class, null);
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
	public <F extends Factor<F>> JavaPairRDD<String, F> stocking(Calculator calc, Class<F> factor, DataDetail<F> detail,
			float expandPartitions, FactorFilter... filters) {
		String[] values = this.values;
		if (values == null) values = new String[0];
		JavaRDD<String> records = calc.sc.parallelize(Arrays.asList(values));
		if (expandPartitions > 1) records = records.repartition((int) Math.ceil(records.partitions().size() * expandPartitions));
		return records.mapToPair(
				(final String t) -> null == t ? null : new Tuple2<>(UUID.randomUUID().toString(), (F) Reflections.construct(factor, t)));
	}
}