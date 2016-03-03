package net.butfly.albacore.calculus;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.jongo.Jongo;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import com.jcabi.log.Logger;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import net.butfly.albacore.calculus.Calculating.Mode;
import net.butfly.albacore.calculus.Functor.Type;
import net.butfly.albacore.calculus.datasource.CalculatorDataSource.HbaseDataSource;
import net.butfly.albacore.calculus.datasource.CalculatorDataSource.KafkaDataSource;
import net.butfly.albacore.calculus.datasource.CalculatorDataSource.MongoDataSource;

public class SparkCalculator {
	// private static org.reflections.Reflections ref = new
	// org.reflections.Reflections(new ConfigurationBuilder());

	public static void main(String[] args) throws Exception {
		final Properties props = new Properties();
		props.load(Thread.currentThread().getContextClassLoader()
				.getResourceAsStream(args.length >= 2 ? args[1] : "calculus.properties"));
		for (String key : System.getProperties().stringPropertyNames())
			if (key.startsWith("calculus.")) props.put(key, System.getProperty(key));
		scanCalculus(props);
	}

	private static Map<String, Properties> subprops(Properties props, String prefix) {
		Map<String, Properties> r = new HashMap<>();
		for (String key : props.stringPropertyNames()) {
			if (!key.startsWith(prefix)) continue;
			String seg = key.substring(prefix.length());
			int pos = seg.indexOf('.');
			String mainkey = seg.substring(0, pos);
			String subkey = seg.substring(pos + 1, seg.length());
			if (!r.containsKey(mainkey)) r.put(mainkey, new Properties());
			r.get(mainkey).put(subkey, props.getProperty(key));
		}
		return r;
	}

	@SuppressWarnings("deprecation")
	private static void scanCalculus(Properties props) throws Exception {
		final CalculatorConfig conf = new CalculatorConfig();
		conf.validate = Boolean.parseBoolean(props.getProperty("calculus.validate.table", "false"));
		final String appname = props.getProperty("calculus.app.name", "Calculuses");
		// dadatabse configurations parsing
		final Map<String, Properties> dbs = subprops(props, "calculus.db.");
		for (String dbid : dbs.keySet()) {
			Properties dbprops = dbs.get(dbid);
			Type type = Type.valueOf(dbprops.getProperty("type"));
			switch (type) {
			case HBASE:
				HbaseDataSource h = new HbaseDataSource();
				h.configFile = dbprops.getProperty("config", "hbase-site.xml");
				conf.datasources.put(dbid, h);
				break;
			case MONGODB:
				MongoDataSource m = new MongoDataSource();
				m.uri = dbprops.getProperty("uri");
				m.authuri = dbprops.getProperty("authuri", m.uri);
				m.db = dbprops.getProperty("db");
				m.client = new MongoClient(new MongoClientURI(m.uri));
				m.mongo = m.client.getDB(m.db);
				m.jongo = new Jongo(m.mongo);
				conf.datasources.put(dbid, m);
				break;
			case KAFKA:
				KafkaDataSource k = new KafkaDataSource();
				k.quonum = dbprops.getProperty("quonum");
				k.group = appname;
				conf.datasources.put(dbid, k);
				break;
			default:
				Logger.warn(SparkCalculator.class, "Unsupportted type: " + type);
			}
		}
		// spark configurations parsing
		if (props.containsKey("calculus.spark.executor.instances"))
			System.setProperty("SPARK_EXECUTOR_INSTANCES", props.getProperty("calculus.spark.executor.instances"));
		SparkConf sconf = new SparkConf();
		sconf.setMaster(props.getProperty("calculus.spark.url"));
		sconf.setAppName(appname + "-Spark");
		if (props.containsKey("calculus.spark.jars")) sconf.setJars(props.getProperty("calculus.spark.jars").split(","));
		if (props.containsKey("calculus.spark.home")) sconf.setSparkHome(props.getProperty("calculus.spark.home"));
		if (props.containsKey("calculus.spark.files")) sconf.set("spark.files", props.getProperty("calculus.spark.files"));
		if (props.containsKey("calculus.spark.executor.memory.mb"))
			sconf.set("spark.executor.memory", props.getProperty("calculus.spark.executor.memory.mb"));
		if (props.containsKey("calculus.spark.testing"))
			sconf.set("spark.testing", props.getProperty("calculus.spark.testing"));
		conf.sc = new JavaSparkContext(sconf);
		conf.ssc = new JavaStreamingContext(conf.sc,
				Durations.seconds(Integer.parseInt(props.getProperty("calculus.spark.duration.seconds", "5"))));
		// scan and run calculuses
		FilterBuilder filterBuilder = new FilterBuilder().includePackage(props.getProperty("calculus.package", ""));
		org.reflections.Reflections ref = new org.reflections.Reflections(
				new ConfigurationBuilder().filterInputsBy(filterBuilder).setUrls(ClasspathHelper.forClassLoader()).addScanners(
						new MethodAnnotationsScanner().filterResultsBy(filterBuilder), new SubTypesScanner(false)));
		for (Class<?> c : ref.getTypesAnnotatedWith(Calculating.class)) {
			// new Task<Void>(new Task.Callable<Void>() {
			// @Override
			// public Void call() throws Exception {
			Calculus calc = (Calculus) c.newInstance();
			new Calculator(calc, conf).calculate(calc, Mode.valueOf(props.getProperty("calculus.mode", "STREAMING")));
			// return null;
			// }
			// }, new Options().fork()).execute();
		}
	}
}
