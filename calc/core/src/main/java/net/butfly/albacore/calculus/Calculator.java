package net.butfly.albacore.calculus;

import java.io.Serializable;
import java.util.Date;
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

import com.google.common.base.CaseFormat;

import net.butfly.albacore.calculus.datasource.ConsoleDataSource;
import net.butfly.albacore.calculus.datasource.ConstDataSource;
import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.datasource.DataSource.DataSources;
import net.butfly.albacore.calculus.datasource.DataSource.Type;
import net.butfly.albacore.calculus.datasource.HbaseDataSource;
import net.butfly.albacore.calculus.datasource.HiveDataSource;
import net.butfly.albacore.calculus.datasource.KafkaDataSource;
import net.butfly.albacore.calculus.datasource.MongoDataSource;
import net.butfly.albacore.calculus.factor.Factors;
import net.butfly.albacore.calculus.utils.Logable;
import net.butfly.albacore.calculus.utils.Maps;
import net.butfly.albacore.calculus.utils.Reflections;

public class Calculator implements Logable, Serializable {
	private static final long serialVersionUID = 7850755405377027618L;
	protected static final Logger logger = LoggerFactory.getLogger(Calculator.class);

	public enum Mode {
		STOCKING, STREAMING
	}

	// devel configurations
	public boolean debug;

	// spark configurations
	public transient SparkConf sconf;
	public transient JavaSparkContext sc;
	public transient JavaStreamingContext ssc;
	private DataSources dss = new DataSources();

	// calculus configurations
	public Mode mode;
	public Class<Calculus> calculusClass;
	public static Calculator calculator;

	public static void main(String... args) {
		CommandLine cmd = commandline(args);
		Properties cprops = new Properties(), sprops = new Properties();
		mergeProps(cprops, sprops, System.getProperties(), Maps.fromFile(cmd.getOptionValue('f', "calculus.properties")), Maps.fromEnv(
				"CALCULUS_", key -> CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_HYPHEN, key).replaceAll("-", ".")));

		if (cmd.hasOption('m')) cprops.setProperty("calculus.mode", cmd.getOptionValue('m').toUpperCase());
		if (cmd.hasOption('c')) cprops.setProperty("calculus.class", cmd.getOptionValue('c'));
		if (cmd.hasOption('d')) cprops.setProperty("calculus.debug", cmd.getOptionValue('d'));
		for (Object key : sprops.keySet())
			System.setProperty(key.toString(), sprops.getProperty(key.toString()));
		for (Object key : cprops.keySet())
			if (System.getProperty(key.toString()) == null) System.setProperty(key.toString(), cprops.getProperty(key.toString()));

		calculator = new Calculator(cprops, sprops);
		calculator.calculate().stop();
	}

	private static void mergeProps(Properties calcProps, Properties sparkProps, Properties... propses) {
		for (Properties p : propses)
			for (String key : p.stringPropertyNames()) {
				if (key.startsWith("calculus.spark.")) {
					String sk = key.substring(9);
					if (!sparkProps.containsKey(sk)) sparkProps.setProperty(sk, p.getProperty(key));
				} else if (!calcProps.containsKey(key)) calcProps.setProperty(key, p.getProperty(key));
			}
	}

	public Calculator stop() {
		if (mode == Calculator.Mode.STREAMING) {
			ssc.start();
			info(() -> calculusClass.getSimpleName() + " streaming started, warting for finish. ");
			try {
				ssc.awaitTermination();
			} catch (Exception e) {
				error(() -> "Streaming interrupted.", e);
			}
			try {
				ssc.close();
			} catch (Exception e) {
				error(() -> "Streaming close failure.", e);
			}
		}
		sc.close();
		return this;
	}

	@SuppressWarnings("unchecked")
	private Calculator(Properties calcProps, Properties sparkProps) {
		mode = Mode.valueOf(calcProps.getProperty("calculus.mode", "STREAMING").toUpperCase());
		debug = Boolean.valueOf(calcProps.getProperty("calculus.debug", "false").toLowerCase());
		if (debug) error(() -> "Running in DEBUG mode, slowly!!!!!");
		if (mode == Calculator.Mode.STOCKING && calcProps.containsKey("calculus.streaming.duration.seconds")) warn(
				() -> "Stocking does not support duration, but duration may be set by calculator for batching.");

		if (!calcProps.containsKey("calculus.class")) throw new IllegalArgumentException(
				"Calculus not defined (-c xxx.ClassName or -Dcalculus.class=xxx.ClassName).");
		// scan and run calculuses
		try {
			calculusClass = (Class<Calculus>) Class.forName(calcProps.getProperty("calculus.class"));
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException("Calculus " + calcProps.getProperty("calculus.class") + " not found or not calculus.", e);
		}
		// dadatabse configurations parsing
		sconf = createSparkConf(sparkProps);
		sc = new JavaSparkContext(sconf);
		ssc = mode == Calculator.Mode.STREAMING ? new JavaStreamingContext(sc, Durations.seconds(Integer.parseInt(calcProps.getProperty(
				"calculus.streaming.duration.seconds", mode == Calculator.Mode.STREAMING ? "30" : "1")))) : null;

		parseDatasources(Maps.aggrProps(Maps.subprops(calcProps, "calculus.ds.", false)));
		debug(() -> "Running " + calculusClass.getSimpleName());
	}

	private SparkConf createSparkConf(Properties sparkProps) {
		SparkConf c = new SparkConf();
		for (String k : sparkProps.stringPropertyNames())
			c.setIfMissing(k, sparkProps.getProperty(k));
		c.setIfMissing("spark.app.name", "Calculus[" + calculusClass.getSimpleName() + "]");
		c.setIfMissing("spark.app.id", calculusClass.getName() + "@" + new Date().toString());
		c.setIfMissing("spark.testing", Boolean.toString(debug));
		c.validateSettings();
		return c;
	}

	private Calculator calculate() {
		info(() -> calculusClass.getSimpleName() + " starting... ");
		long now = new Date().getTime();
		Reflections.construct(calculusClass, this, new Factors()).calculate();
		info(() -> calculusClass.getSimpleName() + " ended, spent: " + (new Date().getTime() - now) + " ms.");
		return this;
	}

	private void parseDatasources(Map<String, Properties> dsprops) {
		for (String dsid : dsprops.keySet()) {
			Properties dbprops = dsprops.get(dsid);
			Type type = Type.valueOf(dbprops.getProperty("type"));
			CaseFormat srcf = dbprops.containsKey("field.name.format.src") ? CaseFormat.valueOf(dbprops.getProperty(
					"field.name.format.src")) : CaseFormat.LOWER_CAMEL;
			CaseFormat dstf = dbprops.containsKey("field.name.format.dst") ? CaseFormat.valueOf(dbprops.getProperty(
					"field.name.format.dst")) : CaseFormat.UPPER_UNDERSCORE;
			String schema = dbprops.getProperty("schema");
			DataSource<?, ?, ?, ?, ?> ds = null;
			switch (type) {
			case HIVE:
				ds = new HiveDataSource(schema, sc, srcf, dstf);
				break;
			case CONST:
				String[] values;
				if (dbprops.containsKey("values")) values = dbprops.getProperty("values").split(dbprops.getProperty("values.split", ","));
				else if (dbprops.containsKey("files")) values = ConsoleDataSource.readLines(dbprops.getProperty("files").split(","));
				else values = ConsoleDataSource.readLines();
				ds = new ConstDataSource(values, srcf, dstf);
				break;
			case HBASE:
				ds = new HbaseDataSource(dbprops.getProperty("config", "hbase-site.xml"), srcf, dstf);
				break;
			case MONGODB:
				String tableSuffix = dbprops.getProperty("output.suffix");
				boolean tableValid = Boolean.parseBoolean(dbprops.getProperty("validate", "true"));
				ds = new MongoDataSource(dbprops.getProperty("uri"), schema, tableSuffix, tableValid, srcf, dstf);
				break;
			case KAFKA:
				String group = debug ? sconf.get("spark.app.name") + UUID.randomUUID().toString() : sconf.get("spark.app.name");
				ds = new KafkaDataSource(dbprops.getProperty("servers"), dbprops.getProperty("schema"), Integer.parseInt(dbprops
						.getProperty("topic.partitions", "1")), group, srcf, dstf);
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

	private static CommandLine commandline(String... args) {
		PosixParser parser = new PosixParser();
		Options opts = new Options();
		opts.addOption("f", "config", true, "Calculus configuration file location. Defalt calculus.properties in classpath schema.");
		opts.addOption("c", "class", true, "Calculus class to be calculated.");
		opts.addOption("m", "mode", true, "Calculating mode, STOCKING or STREAMING. Default STREAMING.");
		opts.addOption("d", "debug", true, "Debug mode, TRUE or FALSE. Default FALSE.");
		opts.addOption("h", "help", false, "Print help information like this.");

		CommandLine cmd;
		try {
			cmd = parser.parse(opts, args);
		} catch (ParseException e) {
			new HelpFormatter().printHelp("java net.butfly.albacore.calculus.Calculator xxx.xxx.XxxCalculus [option]...", opts);
			return null;
		}
		if (cmd.hasOption('h')) new HelpFormatter().printHelp(
				"java net.butfly.albacore.calculus.Calculator xxx.xxx.XxxCalculus [option]...", opts);
		return cmd;
	}

	@SuppressWarnings("rawtypes")
	public <DS extends DataSource> DS getDS(String dbid) {
		DS ds = dss.ds(dbid);
		if (null == ds) logger.warn("Datasource " + dbid + " not configurated, ignore!");
		return ds;
	}
}
