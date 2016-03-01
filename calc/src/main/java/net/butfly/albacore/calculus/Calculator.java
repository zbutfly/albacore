package net.butfly.albacore.calculus;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.jongo.Jongo;
import org.reflections.util.ConfigurationBuilder;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import net.butfly.albacore.calculus.CalculatorConfig.HbaseConfig;
import net.butfly.albacore.calculus.CalculatorConfig.KafkaConfig;
import net.butfly.albacore.calculus.CalculatorConfig.MongodbConfig;
import net.butfly.albacore.calculus.Calculus.Mode;
import net.butfly.albacore.calculus.Functor.Type;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.async.Options;
import net.butfly.albacore.utils.async.Task;

public class Calculator {
	private static org.reflections.Reflections ref = new org.reflections.Reflections(new ConfigurationBuilder());

	public static void main(String[] args) throws Exception {
		final Properties props = new Properties();
		props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(args.length >= 2 ? args[1] : "calculus.properties"));
		for (String key : System.getProperties().stringPropertyNames())
			if (key.startsWith("calculus.")) props.put(key, System.getProperty(key));
		scanCalculus(props);
	}

	private static Map<String, Properties> subprops(Properties props, String prefix) {
		Map<String, Properties> r = new HashMap<>();
		for (String key : props.stringPropertyNames()) {
			if (!key.startsWith(prefix)) continue;
			String mainkey = prefix;
			if (mainkey.endsWith(".")) mainkey = mainkey.substring(0, mainkey.length() - 1);
			String subkey = key.substring(prefix.length());
			if (subkey.startsWith(".")) subkey = subkey.substring(1);

			if (!r.containsKey(mainkey)) r.put(mainkey, new Properties());
			r.get(mainkey).put(subkey, props.getProperty(key));
		}
		return r;
	}

	@SuppressWarnings("deprecation")
	private static void scanCalculus(Properties props) throws Exception {
		final CalculatorConfig conf = new CalculatorConfig();
		final String appname = props.getProperty("calculus.app.name", "Calculuses");
		final Map<String, Properties> dbs = subprops(props, "calculus.db.");
		for (String dbid : dbs.keySet()) {
			Properties dbprops = dbs.get(dbid);
			Type type = Type.valueOf(dbprops.getProperty("type"));
			switch (type) {
			case HBASE:
				HbaseConfig h = new HbaseConfig();
				h.config = props.getProperty("config", "hbase-site.xml");
				conf.hbases.put(dbid, h);
				break;
			case MONGODB:
				MongodbConfig m = new MongodbConfig();
				m.uri = props.getProperty("uri");
				m.authuri = props.getProperty("authuri");
				m.db = props.getProperty("db");
				m.client = new MongoClient(new MongoClientURI(m.uri));
				m.mongo = m.client.getDB(m.db);
				m.jongo = new Jongo(m.mongo);
				break;
			case KAFKA:
				KafkaConfig k = new KafkaConfig();
				k.quonum = props.getProperty("quonum");
				k.group = props.getProperty(appname);
				conf.kafkas.put(dbid, k);
				break;
			default:
				throw new IllegalArgumentException("Unsupportted type: " + type);
			}
		}
		conf.sc = new JavaSparkContext(props.getProperty("calculus.spark.url"), appname + "-Spark");
		conf.ssc = new JavaStreamingContext(conf.sc,
				Durations.seconds(Integer.parseInt(props.getProperty("calculus.spark.duration.seconds", "5"))));
		for (Class<?> c : ref.getTypesAnnotatedWith(Calculus.class))
			new Task<Void>(new Task.Callable<Void>() {
				@Override
				public Void call() throws Exception {
					((CalculusBase) Reflections.construct(c, conf))
							.calculate(Mode.valueOf(props.getProperty("calculus.mode", "STREAMING")));
					return null;
				}
			}, new Options().fork()).execute();
	}
}
