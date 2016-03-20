package net.butfly.albacore.calculus;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;
import net.butfly.albacore.calculus.Calculating.Mode;
import net.butfly.albacore.calculus.datasource.ConstDataSource;
import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.datasource.DataSource.DataSources;
import net.butfly.albacore.calculus.datasource.HbaseDataSource;
import net.butfly.albacore.calculus.datasource.HbaseDetail;
import net.butfly.albacore.calculus.datasource.KafkaDataSource;
import net.butfly.albacore.calculus.datasource.KafkaDetail;
import net.butfly.albacore.calculus.datasource.MongoDataSource;
import net.butfly.albacore.calculus.datasource.MongoDetail;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factor.Stocking;
import net.butfly.albacore.calculus.factor.Factor.Stocking.OnStreaming;
import net.butfly.albacore.calculus.factor.Factor.Streaming;
import net.butfly.albacore.calculus.factor.Factor.Type;
import net.butfly.albacore.calculus.factor.FactorConfig;
import net.butfly.albacore.calculus.factor.Factors;
import net.butfly.albacore.calculus.marshall.Marshaller;
import net.butfly.albacore.calculus.streaming.JavaConstPairDStream;
import net.butfly.albacore.calculus.streaming.JavaReloadPairDStream;
import net.butfly.albacore.calculus.utils.Reflections;

public class Calculator implements Serializable {
	private static final long serialVersionUID = 7850755405377027618L;

	private static final Logger logger = LoggerFactory.getLogger(Calculator.class);
	private static SparkConf sconf;
	private static JavaSparkContext sc;
	private static JavaStreamingContext ssc;
	private static DataSources datasources = new DataSources();

	public boolean validate;
	private int dura;
	private Set<Class<?>> calculuses;
	private Mode mode;
	public static boolean debug;

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
		if (cmd.hasOption('c')) props.setProperty("calculus.classes", cmd.getOptionValue('c'));
		if (cmd.hasOption('d')) props.setProperty("calculus.debug", cmd.getOptionValue('d'));
		new Calculator(props).start().calculate().finish();
	}

	private Calculator start() {
		sc = new JavaSparkContext(sconf);
		ssc = new JavaStreamingContext(sc, Durations.seconds(dura));
		return this;
	}

	private Calculator finish() {
		if (mode == Mode.STREAMING) {
			ssc.start();
			ssc.awaitTermination();
			ssc.close();
		}
		sc.close();
		return this;
	}

	private Calculator(Properties props) {
		mode = Mode.valueOf(props.getProperty("calculus.mode", "STREAMING").toUpperCase());
		debug = Boolean.valueOf(props.getProperty("calculus.debug", "false").toLowerCase());
		if (mode == Mode.STOCKING && props.containsKey("calculus.spark.duration.seconds"))
			logger.warn("Stocking does not support duration, but calculator may set default duration for batching streaming.");
		dura = mode == Mode.STREAMING ? Integer.parseInt(props.getProperty("calculus.spark.duration.seconds", "5")) : 0;
		validate = Boolean.parseBoolean(props.getProperty("calculus.validate.table", "false"));
		final String appname = props.getProperty("calculus.app.name", "Calculuses");
		// dadatabse configurations parsing
		parseDatasources(appname, subprops(props, "calculus.ds."));
		// spark configurations parsing
		if (props.containsKey("calculus.spark.executor.instances"))
			System.setProperty("SPARK_EXECUTOR_INSTANCES", props.getProperty("calculus.spark.executor.instances"));
		sconf = new SparkConf();
		sconf.setMaster(props.getProperty("calculus.spark.url"));
		sconf.setAppName(appname + "-Spark");
		sconf.set("spark.app.id", appname + "Spark-App");
		if (props.containsKey("calculus.spark.jars")) sconf.setJars(props.getProperty("calculus.spark.jars").split(","));
		if (props.containsKey("calculus.spark.home")) sconf.setSparkHome(props.getProperty("calculus.spark.home"));
		if (props.containsKey("calculus.spark.files")) sconf.set("spark.files", props.getProperty("calculus.spark.files"));
		if (props.containsKey("calculus.spark.executor.memory.mb"))
			sconf.set("spark.executor.memory", props.getProperty("calculus.spark.executor.memory.mb"));
		if (props.containsKey("calculus.spark.testing")) sconf.set("spark.testing", props.getProperty("calculus.spark.testing"));

		calculuses = new HashSet<>();
		// scan and run calculuses
		if (props.containsKey("calculus.classes")) {
			for (String cn : props.getProperty("calculus.classes").split(",")) {
				Class<?> c;
				try {
					c = Class.forName(cn);
				} catch (ClassNotFoundException e) {
					logger.warn("Class not found: " + e.toString() + ", ignored.");
					continue;
				}
				if (Calculus.class.isAssignableFrom(c) && c.isAnnotationPresent(Calculating.class)) {
					calculuses.add(c);
					logger.debug("Found: " + c.toString());
				} else logger.warn("Ignore: " + c.toString() + ", either not Calculus or not annotated by @Calculating.");

			}
		} else {
			FastClasspathScanner scaner = props.containsKey("calculus.package") ? new FastClasspathScanner()
					: new FastClasspathScanner(props.getProperty("calculus.package").split(","));
			scaner.matchClassesWithAnnotation(Calculating.class, c -> {}).matchClassesImplementing(Calculus.class, c -> {
				calculuses.add(c);
			}).scan();
		}
		logger.debug("Running calculuses: " + calculuses.toString());
	}

	@SuppressWarnings("unchecked")
	private <OUTK, OUTV extends Factor<OUTV>> Calculator calculate() {
		for (Class<?> klass : calculuses) {
			Calculating calcing = klass.getAnnotation(Calculating.class);
			Calculus<OUTK, OUTV> calc;
			try {
				calc = (Calculus<OUTK, OUTV>) klass.newInstance();
			} catch (Exception e) {
				logger.error("Calculus " + klass.toString() + " constructor failure, ignored", e);
				continue;
			}
			List<FactorConfig<?, ?>> configs = new ArrayList<>();
			FactorConfig<?, ?> batch = null;
			for (@SuppressWarnings("rawtypes")
			Class ccc : calcing.value()) {
				FactorConfig<?, ?> c = scan(mode, ccc);
				if (null == c) continue;
				if (c.batching > 0) {
					if (batch != null) throw new IllegalArgumentException("Only one batch stocking source supported, now found second: "
							+ batch.factorClass.toString() + " and " + c.factorClass.toString());
					else batch = c;
				}
				configs.add(c);
			}

			Factors factors = new Factors();
			for (FactorConfig<?, ?> c : configs)
				if (c != null) read(factors, c);
			logger.info("Calculus " + klass.toString() + " starting... ");
			FactorConfig<OUTK, OUTV> saving = scan(Mode.STOCKING,
					Reflections.resolveGenericParameter(calc.getClass(), Calculus.class, "OUTV"));
			calc.streaming(ssc, factors, datasources.get(saving.dbid).saving(sc, saving.detail));
			logger.info("Calculus " + klass.toString() + " started. ");
		}
		return this;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private <K, F extends Factor<F>> FactorConfig<K, F> scan(Mode mode, Class<F> factor) {
		FactorConfig<K, F> config = new FactorConfig<K, F>();
		config.factorClass = factor;
		if (mode == Mode.STREAMING && factor.isAnnotationPresent(Streaming.class)) {
			config.mode = Mode.STREAMING;
			Streaming s = factor.getAnnotation(Streaming.class);
			config.dbid = s.source();
			switch (s.type()) {
			case KAFKA:
				config.detail = new KafkaDetail(s.topics());
				break;
			default:
				throw new UnsupportedOperationException("Unsupportted streaming mode: " + s.type() + " on " + factor.toString());
			}
		} else {
			if (!factor.isAnnotationPresent(Stocking.class)) return null;
			Stocking s = factor.getAnnotation(Stocking.class);
			if (mode == Mode.STREAMING && s.streaming() == OnStreaming.NONE) return null;
			config.mode = Mode.STOCKING;
			config.dbid = s.source();
			config.batching = s.batching();
			config.streaming = s.streaming();
			switch (s.type()) {
			case HBASE:
				if (Factor.NOT_DEFINED.equals(s.table()))
					throw new IllegalArgumentException("Table not defined for factor " + factor.toString());
				config.detail = new HbaseDetail(s.table());
				break;
			case MONGODB:
				if (Factor.NOT_DEFINED.equals(s.table()))
					throw new IllegalArgumentException("Table not defined for factor " + factor.toString());
				config.detail = new MongoDetail(s.table(), Factor.NOT_DEFINED.equals(s.filter()) ? null : s.filter());
				break;
			case CONSTAND_TO_CONSOLE:
				break;
			default:
				throw new UnsupportedOperationException("Unsupportted stocking mode: " + s.type() + " on " + factor.toString());
			}
			if (validate) {
				DataSource ds = datasources.get(s.source());
				ds.confirm(factor, config.detail);
			}
		}
		return config;
	}

	private <K, F extends Factor<F>> void read(Factors factors, FactorConfig<K, F> config) {
		switch (mode) {
		case STOCKING:
			if (config.batching <= 0)
				factors.stocking(config.factorClass, datasources.get(config.dbid).stocking(sc, config.factorClass, config.detail), ssc);
			else factors.streaming(config.factorClass, datasources.get(config.dbid).batching(ssc, config.factorClass, config.batching,
					config.detail, config.keyClass, config.factorClass));
			break;
		case STREAMING:
			switch (config.mode) {
			case STOCKING:
				switch (config.streaming) {
				case NONE:
					break;
				case ONCE:
					factors.streaming(config.factorClass,
							new JavaConstPairDStream<>(ssc, datasources.get(config.dbid).stocking(sc, config.factorClass, config.detail))
									.persist());
					break;
				case EACH:
					factors.streaming(config.factorClass,
							new JavaReloadPairDStream<>(ssc,
									() -> datasources.get(config.dbid).stocking(sc, config.factorClass, config.detail), config.keyClass,
									config.factorClass));
					break;
				case CACHE:
					throw new NotImplementedException();
				}
				break;
			case STREAMING:
				factors.streaming(config.factorClass, datasources.get(config.dbid).streaming(ssc, config.factorClass, config.detail));
				break;
			}
			break;
		}
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

	private static CommandLine commandline(String... args) throws ParseException {
		PosixParser parser = new PosixParser();
		Options opts = new Options();
		opts.addOption("f", "config", true, "Calculus configuration file location. Defalt calculus.properties in classpath root.");
		opts.addOption("c", "classes", true,
				"Calculus classes list to be calculated, splitted by comma. Default scan all subclasses of Calculus, with annotation \"Calculating\".");
		opts.addOption("m", "mode", true, "Calculating mode, STOCKING or STREAMING. Default STREAMING.");
		opts.addOption("d", "debug", true, "Debug mode, TRUE or FALSE. Default FALSE.");
		opts.addOption("h", "help", false, "Print help information like this.");

		CommandLine cmd = parser.parse(opts, args);
		if (cmd.hasOption('h')) new HelpFormatter().printHelp("java net.butfly.albacore.calculus.SparkCalculator [option]...", opts);
		return cmd;
	}

	@SuppressWarnings("unchecked")
	private void parseDatasources(String appname, Map<String, Properties> dsprops) {
		for (String dsid : dsprops.keySet()) {
			Properties dbprops = dsprops.get(dsid);
			Marshaller<?, ?> m;
			try {
				m = (Marshaller<?, ?>) Class.forName(dbprops.getProperty("marshaller")).newInstance();
			} catch (Exception e) {
				m = null;
			}
			Type type = Type.valueOf(dbprops.getProperty("type"));
			switch (type) {
			case CONSTAND_TO_CONSOLE:
				datasources.put(dsid, new ConstDataSource(dbprops.getProperty("values").split(",")));
				break;
			case HBASE:
				datasources.put(dsid, new HbaseDataSource(dbprops.getProperty("config", "hbase-site.xml"),
						(Marshaller<ImmutableBytesWritable, Result>) m));
				break;
			case MONGODB:
				datasources.put(dsid, new MongoDataSource(dbprops.getProperty("uri"), (Marshaller<Object, BSONObject>) m));
				// , dbprops.getProperty("authdb"),dbprops.getProperty("authdb")
				break;
			case KAFKA:
				datasources.put(dsid,
						new KafkaDataSource(dbprops.getProperty("servers"), dbprops.getProperty("root"),
								Integer.parseInt(dbprops.getProperty("topic.partitions", "1")),
								debug ? appname + UUID.randomUUID().toString() : appname, (Marshaller<String, byte[]>) m));
				break;
			default:
				logger.warn("Unsupportted type: " + type);
			}
		}
	}

	public static final InputStream scanInputStream(String file) throws FileNotFoundException, IOException {
		URL url = Thread.currentThread().getContextClassLoader().getResource(file);
		return null == url ? new FileInputStream(file) : url.openStream();
	}
}
