package net.butfly.albacore.calculus;

import java.io.IOException;
import java.util.Properties;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.reflections.util.ConfigurationBuilder;

import net.butfly.albacore.utils.Reflections;

public class Engine {
	private static org.reflections.Reflections ref = new org.reflections.Reflections(new ConfigurationBuilder());

	public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException {
		final Properties props = new Properties();
		System.setProperties(props);
		props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(args.length >= 2 ? args[1] : "calculus.properties"));
		scanCalculus(props);
	}

	private static void scanCalculus(Properties props) throws InstantiationException, IllegalAccessException, IOException {
		EngineConfig econf = new EngineConfig();
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
				// TODO: launch the calc created.
			}
		} finally {
			econf.ssc.close();
		}
	}
}
