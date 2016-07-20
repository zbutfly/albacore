package net.butfly.albacore.calculus;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.butfly.albacore.calculus.datasource.ConstDataSource;
import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.datasource.DataSource.DataSources;
import net.butfly.albacore.calculus.datasource.HbaseDataSource;
import net.butfly.albacore.calculus.datasource.HiveDataSource;
import net.butfly.albacore.calculus.datasource.KafkaDataSource;
import net.butfly.albacore.calculus.datasource.MongoDataSource;
import net.butfly.albacore.calculus.factor.Factor.Type;
import net.butfly.albacore.calculus.factor.Factors;
import net.butfly.albacore.calculus.marshall.HbaseMarshaller;
import net.butfly.albacore.calculus.marshall.HiveMarshaller;
import net.butfly.albacore.calculus.marshall.KafkaMarshaller;
import net.butfly.albacore.calculus.marshall.Marshaller;
import net.butfly.albacore.calculus.marshall.MongoMarshaller;
import net.butfly.albacore.calculus.utils.Logable;
import net.butfly.albacore.calculus.utils.Reflections;

public class Calculator implements Logable, Serializable {
	private static final long serialVersionUID = 7850755405377027618L;
	protected static final Logger logger = LoggerFactory.getLogger(Calculator.class);

	// devel configurations
	public boolean debug;

	// spark configurations
	public transient SparkConf sconf;
	public transient JavaSparkContext sc;
	public transient JavaStreamingContext ssc;
	public DataSources dss = new DataSources();
	private int dura;

	// calculus configurations
	public Mode mode;
	public Class<Calculus> calculusClass;
	private String appname;

	public static void main(String... args) {
		final Properties props = new Properties();
		CommandLine cmd;
		try {
			cmd = commandline(args);
			props.load(scanInputStream(cmd.getOptionValue('f', "calculus.properties")));
		} catch (IOException | ParseException e) {
			throw new RuntimeException(e);
		}
		for (String key : System.getProperties().stringPropertyNames())
			if (key.startsWith("calculus.")) props.put(key, System.getProperty(key));
		if (cmd.hasOption('m')) props.setProperty("calculus.mode", cmd.getOptionValue('m').toUpperCase());
		if (cmd.hasOption('c')) props.setProperty("calculus.class", cmd.getOptionValue('c'));
		if (cmd.hasOption('d')) props.setProperty("calculus.debug", cmd.getOptionValue('d'));
		for (Object key : props.keySet())
			System.setProperty(key.toString(), props.getProperty(key.toString()));
		Calculator c = new Calculator(props);
		c.calculate().end();
	}

	private Calculator end() {
		if (mode == Mode.STREAMING) {
			ssc.start();
			info(() -> calculusClass.getSimpleName() + " streaming started, warting for finish. ");
			ssc.awaitTermination();
			try {
				ssc.close();
			} catch (Throwable th) {
				error(() -> "Streaming error", th);
			}
		}
		sc.close();
		return this;
	}

	@SuppressWarnings("unchecked")
	private Calculator(Properties props) {
		mode = Mode.valueOf(props.getProperty("calculus.mode", "STREAMING").toUpperCase());
		debug = Boolean.valueOf(props.getProperty("calculus.debug", "false").toLowerCase());
		if (debug) error(() -> "Running in DEBUG mode, slowly!!!!!");
		if (mode == Mode.STOCKING && props.containsKey("calculus.spark.duration.seconds"))
			warn(() -> "Stocking does not support duration, but duration may be set by calculator for batching.");
		dura = mode == Mode.STREAMING ? Integer.parseInt(props.getProperty("calculus.spark.duration.seconds", "30"))
				: Integer.parseInt(props.getProperty("calculus.spark.duration.seconds", "1"));
		if (!props.containsKey("calculus.class"))
			throw new IllegalArgumentException("Calculus not defined (-c xxx.ClassName or -Dcalculus.class=xxx.ClassName).");
		// scan and run calculuses
		try {
			calculusClass = (Class<Calculus>) Class.forName(props.getProperty("calculus.class"));
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException("Calculus " + props.getProperty("calculus.class") + " not found or not calculus.", e);
		}
		this.appname = props.getProperty("calculus.app.name", "Calculuses:" + calculusClass.getSimpleName());
		// dadatabse configurations parsing
		sconf = new SparkConf();
		if (props.containsKey("calculus.spark.url")) sconf.setMaster(props.getProperty("calculus.spark.url"));
		sconf.setAppName(appname + "-Spark");
		if (props.containsKey("calculus.spark.jars")) sconf.setJars(props.getProperty("calculus.spark.jars").split(","));
		sconf.set("spark.app.id", appname + "[Spark-App]");
		sconf.set("spark.testing", Boolean.toString(false));
		if (props.containsKey("calculus.spark.jars")) sconf.setJars(props.getProperty("calculus.spark.jars").split(","));
		if (props.containsKey("calculus.spark.home")) sconf.setSparkHome(props.getProperty("calculus.spark.home"));
		if (debug) sconf.set("spark.testing", "true");
		sc = new JavaSparkContext(sconf);
		if (mode == Mode.STREAMING) ssc = new JavaStreamingContext(sc, Durations.seconds(dura));
		parseDatasources(subprops(props, "calculus.ds."));
		debug(() -> "Running " + calculusClass.getSimpleName());
	}

	private Calculator calculate() {
		info(() -> calculusClass.getSimpleName() + " starting... ");
		long now = new Date().getTime();
		Reflections.construct(calculusClass, this, new Factors(this)).calculate();
		info(() -> calculusClass.getSimpleName() + " ended, spent: " + (new Date().getTime() - now) + " ms.");
		return this;
	}

	private Map<String, Properties> subprops(Properties props, String prefix) {
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

	private void parseDatasources(Map<String, Properties> dsprops) {
		for (String dsid : dsprops.keySet()) {
			Properties dbprops = dsprops.get(dsid);
			Marshaller<?, ?, ?> m;
			try {
				m = (Marshaller<?, ?, ?>) Class.forName(dbprops.getProperty("marshaller")).newInstance();
			} catch (Exception e) {
				m = null;
			}
			Type type = Type.valueOf(dbprops.getProperty("type"));
			DataSource<?, ?, ?, ?, ?> ds = null;
			switch (type) {
			case HIVE:
				ds = new HiveDataSource(dbprops.getProperty("schema"), (HiveMarshaller) m, this.sc);
				break;
			case CONSTAND_TO_CONSOLE:
				ds = new ConstDataSource(dbprops.getProperty("values").split(","));
				break;
			case HBASE:
				ds = new HbaseDataSource(dbprops.getProperty("config", "hbase-site.xml"), (HbaseMarshaller) m);
				break;
			case MONGODB:
				ds = new MongoDataSource(dbprops.getProperty("uri"), (MongoMarshaller) m, dbprops.getProperty("output.suffix"),
						Boolean.parseBoolean(dbprops.getProperty("validate", "true")));
				break;
			case KAFKA:
				ds = new KafkaDataSource(dbprops.getProperty("servers"), dbprops.getProperty("root"),
						Integer.parseInt(dbprops.getProperty("topic.partitions", "1")),
						debug ? appname + UUID.randomUUID().toString() : appname, (KafkaMarshaller) m);
				break;
			default:
				warn(() -> "Unsupportted type: " + type);
			}
			if (null != ds) {
				ds.debugLimit = Integer.parseInt(dbprops.getProperty("debug.limit", "0"));
				ds.debugRandomChance = Float.parseFloat(dbprops.getProperty("debug.random", "0"));
				dss.put(dsid, ds);
			}
		}
	}

	private static CommandLine commandline(String... args) throws ParseException {
		PosixParser parser = new PosixParser();
		Options opts = new Options();
		opts.addOption("f", "config", true, "Calculus configuration file location. Defalt calculus.properties in classpath root.");
		opts.addOption("c", "class", true, "Calculus class to be calculated.");
		opts.addOption("m", "mode", true, "Calculating mode, STOCKING or STREAMING. Default STREAMING.");
		opts.addOption("d", "debug", true, "Debug mode, TRUE or FALSE. Default FALSE.");
		opts.addOption("h", "help", false, "Print help information like this.");

		CommandLine cmd = parser.parse(opts, args);
		if (cmd.hasOption('h'))
			new HelpFormatter().printHelp("java net.butfly.albacore.calculus.Calculator xxx.xxx.XxxCalculus [option]...", opts);
		return cmd;
	}

	public static final InputStream scanInputStream(String file) throws FileNotFoundException, IOException {
		URL url = Thread.currentThread().getContextClassLoader().getResource(file);
		return null == url ? new FileInputStream(file) : url.openStream();
	}
}
