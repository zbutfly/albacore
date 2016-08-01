package net.butfly.albacore.calculus.datasource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.spark.api.java.JavaRDD;

import com.google.common.base.CaseFormat;
import com.google.common.base.Joiner;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.FactroingConfig;
import net.butfly.albacore.calculus.factor.Factoring.Type;
import net.butfly.albacore.calculus.factor.filter.FactorFilter;
import net.butfly.albacore.calculus.factor.rds.PairRDS;
import net.butfly.albacore.calculus.factor.rds.internal.WrappedRDD;
import net.butfly.albacore.calculus.utils.Reflections;
import scala.Tuple2;

public class ConstDataSource extends DataSource<String, Void, String, Void, String> {
	private static final long serialVersionUID = -673387208224779163L;
	private String[] values;

	public ConstDataSource(String[] values, CaseFormat srcf, CaseFormat dstf) {
		super(Type.CONSOLE, null, false, null, Void.class, String.class, NullOutputFormat.class, null, srcf, dstf);
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
	public <F extends Factor<F>> PairRDS<String, F> stocking(Calculator calc, Class<F> factor, FactroingConfig<F> detail, float expandPartitions,
			FactorFilter... filters) {
		String[] values = this.values;
		if (values == null) values = new String[0];
		JavaRDD<String> records = calc.sc.parallelize(Arrays.asList(values));
		if (expandPartitions > 0) records = records.repartition((int) Math.ceil(records.getNumPartitions() * expandPartitions));
		return new PairRDS<>(new WrappedRDD<>(records.mapToPair(
				(final String t) -> null == t ? null : new Tuple2<>(UUID.randomUUID().toString(), (F) Reflections.construct(factor, t)))));
	}

	public static String[] readLines(String... uri) {
		List<String> lines = new ArrayList<>();
		try {
			if (null == uri || uri.length == 0) appendAll(lines, System.in);
			else for (String u : uri)
				appendAll(lines, new URI(u).toURL().openStream());
		} catch (IOException | URISyntaxException ex) {
			throw new RuntimeException(ex);
		}
		return lines.toArray(new String[lines.size()]);
	}

	private static void appendAll(List<String> sb, InputStream is) throws IOException {
		String line;
		BufferedReader r = new BufferedReader(new InputStreamReader(is));
		try {
			while ((line = r.readLine()) != null)
				sb.add(line);
		} finally {
			r.close();
		}
	}

	@Override
	public String andQuery(String... ands) {
		throw new UnsupportedOperationException();
	}
}