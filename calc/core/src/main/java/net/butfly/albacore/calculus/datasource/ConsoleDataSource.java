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

import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.dstream.DStream;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.FactroingConfig;
import net.butfly.albacore.calculus.factor.filter.FactorFilter;
import net.butfly.albacore.calculus.factor.rds.PairRDS;
import net.butfly.albacore.calculus.factor.rds.internal.RDSupport;
import net.butfly.albacore.calculus.factor.rds.internal.WrappedRDD;
import net.butfly.albacore.calculus.utils.Reflections;
import scala.Option;
import scala.Tuple2;

public class ConsoleDataSource extends DataSource<String, Void, String, Void, String> {
	private static final long serialVersionUID = -673387208224779163L;

	private ConsoleDataSource() {
		super(Type.CONSOLE, null, false, null, Void.class, String.class, OutputFormat.class, null, null, null);
	}

	@Override
	public <F extends Factor<F>> PairRDS<String, F> stocking(Class<F> factor, FactroingConfig<F> detail, float expandPartitions,
			FactorFilter... filters) {
		JavaRDD<String> records = Calculator.calculator.sc.parallelize(Arrays.asList(readLines()));
		if (expandPartitions > 0) records = records.repartition((int) Math.ceil(records.getNumPartitions() * expandPartitions));
		return new PairRDS<>(new WrappedRDD<>(records.mapToPair(t -> null == t ? null
				: new Tuple2<>(null, (F) Reflections.construct(factor, t)))));
	}

	@Override
	public <F extends Factor<F>> JavaPairDStream<String, F> streaming(Class<F> factor, FactroingConfig<F> detail, FactorFilter... filters) {
		return JavaPairDStream.fromPairDStream(new ConsoleInputDStream<F>(), RDSupport.tag(String.class), RDSupport.tag(factor));
	}

	@Override
	public void save(JavaPairRDD<Void, String> w, FactroingConfig<?> dd) {
		w.foreach(t -> System.out.print(t._2));
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

	private static class ConsoleInputDStream<F> extends DStream<Tuple2<String, F>> {
		private BufferedReader reader;

		public ConsoleInputDStream() {
			super(Calculator.calculator.ssc.ssc(), RDSupport.tag());
			this.reader = new BufferedReader(new InputStreamReader(System.in));
		}

		@Override
		public Option<RDD<Tuple2<String, F>>> compute(Time time) {
			List<Tuple2<String, F>> t = new ArrayList<>();
			String line;
			do {
				try {
					line = reader.readLine();
				} catch (IOException e) {
					return compute(t);
				}
				if (line == null || line.length() == 0) return compute(t);
				t.add(new Tuple2<>(line, null));
				if (t.size() > 5) return compute(t);
			} while (true);
		}

		private Option<RDD<Tuple2<String, F>>> compute(List<Tuple2<String, F>> t) {
			return t.size() == 0 ? Option.empty() : Option.apply(Calculator.calculator.sc.parallelize(t).rdd());
		}

		@Override
		public scala.collection.immutable.List<DStream<?>> dependencies() {
			return null;
		}

		@Override
		public Duration slideDuration() {
			return null;
		}
	}

	public final static ConsoleDataSource CONSOLE_DS = new ConsoleDataSource();

}