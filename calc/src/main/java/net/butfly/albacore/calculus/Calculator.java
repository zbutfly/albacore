package net.butfly.albacore.calculus;

import java.util.Properties;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.reflections.util.ConfigurationBuilder;

import net.butfly.albacore.calculus.Calculus.Mode;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.async.Task;

public class Calculator {
	private static org.reflections.Reflections ref = new org.reflections.Reflections(new ConfigurationBuilder());

	public static void main(String[] args) throws Exception {
		final Properties props = new Properties();
		System.setProperties(props);
		props.load(Thread.currentThread().getContextClassLoader()
				.getResourceAsStream(args.length >= 2 ? args[1] : "calculus.properties"));
		scanCalculus(props);
	}

	private static void scanCalculus(Properties props) throws Exception {
		CalculatorConfig econf = new CalculatorConfig();
		econf.sc = new JavaSparkContext(props.getProperty("calculus.spark.url"),
				props.getProperty("calculus.spark.app.name", "Calculus Engine"));
		econf.sqsc = new SQLContext(econf.sc);
		econf.ssc = new JavaStreamingContext(econf.sc,
				Durations.seconds(Integer.parseInt(props.getProperty("calculus.spark.duration.seconds", "0"))));
		econf.hconfig = props.getProperty("calculus.hbase.config", "hbase/hbase-site.xml");
		econf.kquonum = props.getProperty("calculus.kafka.quonum");
		econf.kgroup = props.getProperty("calculus.kafka.group");
		try {
			for (Class<?> c : ref.getTypesAnnotatedWith(Calculus.class)) {
				CalculusBase calc = (CalculusBase) Reflections.construct(c, econf);
				new Task<Void>(new Task.Callable<Void>() {
					@Override
					public Void call() throws Exception {
						calc.calculate(Mode.STOCKING);
						return null;
					}
				}).execute();
			}
		} finally {
			econf.ssc.close();
		}
	}
}
