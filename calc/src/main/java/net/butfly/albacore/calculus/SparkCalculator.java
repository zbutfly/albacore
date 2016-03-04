package net.butfly.albacore.calculus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.bson.BSONObject;
import org.jongo.Jongo;
import org.jongo.MongoCollection;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.MongoInputFormat;

import net.butfly.albacore.calculus.Calculating.Mode;
import net.butfly.albacore.calculus.CalculatorContext.SparkCalculatorContext;
import net.butfly.albacore.calculus.Functor.Stocking;
import net.butfly.albacore.calculus.Functor.Streaming;
import net.butfly.albacore.calculus.Functor.Type;
import net.butfly.albacore.calculus.datasource.CalculatorDataSource;
import net.butfly.albacore.calculus.datasource.CalculatorDataSource.ConstDataSource;
import net.butfly.albacore.calculus.datasource.CalculatorDataSource.HbaseDataSource;
import net.butfly.albacore.calculus.datasource.CalculatorDataSource.KafkaDataSource;
import net.butfly.albacore.calculus.datasource.CalculatorDataSource.MongoDataSource;
import net.butfly.albacore.calculus.marshall.HbaseResultMarshaller;
import net.butfly.albacore.calculus.marshall.KafkaMarshaller;
import net.butfly.albacore.calculus.marshall.MongoMarshaller;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.async.Task;
import scala.Tuple2;

public class SparkCalculator {
	private static final Logger logger = LoggerFactory.getLogger(SparkCalculator.class);

	private SparkCalculatorContext context;
	private Set<Class<?>> calculuses;

	public static void main(String... args) throws Exception {
		final Properties props = new Properties();
		CommandLine cmd = commandline(args);
		props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(cmd.getOptionValue('f', "calculus.properties")));
		for (String key : System.getProperties().stringPropertyNames())
			if (key.startsWith("calculus.")) props.put(key, System.getProperty(key));
		if (cmd.hasOption('m')) props.setProperty("calculus.mode", cmd.getOptionValue('m').toUpperCase());
		if (cmd.hasOption('c')) props.setProperty("calculus.classes", cmd.getOptionValue('c'));

		new SparkCalculator(props).calculate(Mode.valueOf(props.getProperty("calculus.mode", "STREAMING")));
	}

	private SparkCalculator(Properties props) throws Exception {
		context = new SparkCalculatorContext();
		context.validate = Boolean.parseBoolean(props.getProperty("calculus.validate.table", "false"));
		final String appname = props.getProperty("calculus.app.name", "Calculuses");
		// dadatabse configurations parsing
		scanDatasources(appname, subprops(props, "calculus.ds."));
		// spark configurations parsing
		if (props.containsKey("calculus.spark.executor.instances"))
			System.setProperty("SPARK_EXECUTOR_INSTANCES", props.getProperty("calculus.spark.executor.instances"));
		SparkConf sconf = new SparkConf();
		sconf.setMaster(props.getProperty("calculus.spark.url"));
		sconf.setAppName(appname + "-Spark");
		sconf.set("spark.app.id", appname + "Spark-App");
		if (props.containsKey("calculus.spark.jars")) sconf.setJars(props.getProperty("calculus.spark.jars").split(","));
		if (props.containsKey("calculus.spark.home")) sconf.setSparkHome(props.getProperty("calculus.spark.home"));
		if (props.containsKey("calculus.spark.files")) sconf.set("spark.files", props.getProperty("calculus.spark.files"));
		if (props.containsKey("calculus.spark.executor.memory.mb"))
			sconf.set("spark.executor.memory", props.getProperty("calculus.spark.executor.memory.mb"));
		if (props.containsKey("calculus.spark.testing")) sconf.set("spark.testing", props.getProperty("calculus.spark.testing"));
		context.sc = new JavaSparkContext(sconf);
		context.ssc = new JavaStreamingContext(context.sc,
				Durations.seconds(Integer.parseInt(props.getProperty("calculus.spark.duration.seconds", "5"))));
		// scan and run calculuses
		FilterBuilder filterBuilder = new FilterBuilder().includePackage(props.getProperty("calculus.package", ""));
		org.reflections.Reflections ref = new org.reflections.Reflections(
				new ConfigurationBuilder().filterInputsBy(filterBuilder).setUrls(ClasspathHelper.forClassLoader())
						.addScanners(new MethodAnnotationsScanner().filterResultsBy(filterBuilder), new SubTypesScanner(false)));
		if (props.containsKey("calculus.classes")) {
			calculuses = new HashSet<>();
			for (String c : props.getProperty("calculus.classes").split(",")) {
				Class<?> cc = Reflections.forClassName(c);
				if (cc != null && Calculus.class.isAssignableFrom(cc)) calculuses.add(cc);
				else logger.warn(c + " is not Calculus, ignored.");
			}
		} else calculuses = ref.getTypesAnnotatedWith(Calculating.class);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private <F extends Functor<F>> void calculate(Mode mode) throws Exception {
		for (Class<?> c : calculuses) {
			logger.info("Calculus " + c.toString() + " starting... ");
			new Task<Void>(new Task.Callable<Void>() {
				@Override
				public Void call() throws IOException {
					Calculus calc;
					try {
						calc = (Calculus) c.newInstance();
					} catch (Exception e) {
						logger.error("Calculus " + c.toString() + " constructor failure, ignored", e);
						return null;
					}
					Map<Class<? extends Functor<?>>, JavaPairRDD<String, ? extends Functor<?>>> stockingFunctors = new HashMap<>();
					Map<Class<? extends Functor<?>>, JavaPairRDD<String, ? extends Functor<?>>> streamingFunctors = new HashMap<>();
					Map<Class<? extends Functor<?>>, FunctorConfig> stockingConfigs = new HashMap<>();
					Map<Class<? extends Functor<?>>, FunctorConfig> streamingConfigs = new HashMap<>();
					FunctorConfig destConfig = parseFunctorConfigs(calc, stockingConfigs, streamingConfigs);
					switch (mode) {
					case STOCKING:
						for (Class<? extends Functor<?>> functorClass : stockingConfigs.keySet())
							stockingFunctors.put(functorClass,
									(JavaPairRDD<String, ? extends Functor<?>>) stocking((Class<? extends Functor>) functorClass,
											stockingConfigs.get(functorClass)));
						for (Class<? extends Functor<?>> functorClass : streamingConfigs.keySet())
							stockingFunctors.put(functorClass,
									(JavaPairRDD<String, ? extends Functor<?>>) stocking((Class<? extends Functor>) functorClass,
											stockingConfigs.get(functorClass)));
						break;
					case STREAMING:
						for (Class<? extends Functor<?>> functorClass : stockingConfigs.keySet())
							stockingFunctors.put(functorClass,
									(JavaPairRDD<String, ? extends Functor<?>>) stocking((Class<? extends Functor>) functorClass,
											stockingConfigs.get(functorClass)));
						for (Class<? extends Functor<?>> functorClass : streamingConfigs.keySet())
							streamingFunctors.put(functorClass,
									(JavaPairRDD<String, ? extends Functor<?>>) streaming((Class<? extends Functor>) functorClass,
											streamingConfigs.get(functorClass)));
						break;
					}
					JavaRDD<F> r = (JavaRDD<F>) calc.calculate(context.sc, stockingFunctors, streamingFunctors);
					if (destConfig != null && null != r) {
						r.foreach(new WriteFunction(destConfig, context.datasources.get(destConfig.datasource)));
					}
					return null;
				}
			}/* , new net.butfly.albacore.utils.async.Options().fork() */).execute();
			logger.info("Calculus " + c.toString() + " started. ");
		}
	}

	private static class WriteFunction<F extends Functor<F>> implements VoidFunction<F> {
		private static final long serialVersionUID = -3114062446562954303L;
		private FunctorConfig functorConfig;
		private CalculatorDataSource dataSource;

		public WriteFunction(FunctorConfig functorConfig, CalculatorDataSource dataSource) {
			this.functorConfig = functorConfig;
			this.dataSource = dataSource;
		}

		@Override
		public void call(F result) throws Exception {
			switch (functorConfig.functorClass.getAnnotation(Stocking.class).type()) {
			case MONGODB:
				MongoCollection col = ((MongoDataSource) dataSource).jongo.getCollection(functorConfig.mongoTable);
				col.save(result);
				break;
			case CONST:
				logger.info("Result: " + result.toString());
				break;
			default:
				throw new IllegalArgumentException(
						"Write to " + functorConfig.functorClass.getAnnotation(Stocking.class).type() + " is not supported.");
			}
		}
	}

	<F extends Functor<F>> JavaPairRDD<String, F> stocking(Class<F> functorClass, FunctorConfig fucntorConfig) {
		CalculatorDataSource dsc = context.datasources.get(fucntorConfig.datasource);
		switch (functorClass.getAnnotation(Stocking.class).type()) {
		case HBASE: // TODO: adaptor to hbase data frame
			Configuration hconf = HBaseConfiguration.create();
			try {
				hconf.addResource(Thread.currentThread().getContextClassLoader()
						.getResource(((HbaseDataSource) context.datasources.get(fucntorConfig.datasource)).configFile).openStream());
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			hconf.set(TableInputFormat.INPUT_TABLE, fucntorConfig.hbaseTable);
			// conf.hconf.set(TableInputFormat.SCAN_COLUMNS, "cf1:vc cf1:vs");
			final HbaseResultMarshaller hm = (HbaseResultMarshaller) dsc.marshaller;
			return context.sc.newAPIHadoopRDD(hconf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class)
					.mapToPair(t -> new Tuple2<String, F>(hm.unmarshallId(t._1), hm.unmarshall(t._2, functorClass)));
		case MONGODB:
			Configuration mconf = new Configuration();
			MongoDataSource mds = (MongoDataSource) context.datasources.get(fucntorConfig.datasource);
			mconf.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");
			mconf.set("mongo.auth.uri", mds.authuri);
			mconf.set("mongo.input.uri", mds.uri + "." + fucntorConfig.mongoTable);
			if (fucntorConfig.mongoFilter != null && !"".equals(fucntorConfig.mongoFilter))
				mconf.set("mongo.input.query", fucntorConfig.mongoFilter);
			// conf.mconf.set("mongo.input.fields
			mconf.set("mongo.input.notimeout", "true");
			MongoMarshaller mm = (MongoMarshaller) dsc.marshaller;
			return (JavaPairRDD<String, F>) context.sc.newAPIHadoopRDD(mconf, MongoInputFormat.class, Object.class, BSONObject.class)
					.mapToPair(t -> new Tuple2<String, F>(mm.unmarshallId(t._1), mm.unmarshall(t._2, functorClass)));
		case CONST:
			return context.sc.parallelize(Arrays.asList(((ConstDataSource) context.datasources.get(fucntorConfig.datasource)).values))
					.mapToPair(t -> new Tuple2<String, F>(UUID.randomUUID().toString(), (F) Reflections.construct(functorClass, t)));
		default:
			throw new IllegalArgumentException("Unsupportted stocking mode: " + functorClass.getAnnotation(Stocking.class).type());
		}
	}

	<F extends Functor<F>> JavaPairRDD<String, F> streaming(Class<F> functorClass, FunctorConfig fucntorConfig) throws IOException {
		Streaming streaming = functorClass.getAnnotation(Streaming.class);
		CalculatorDataSource dsc = context.datasources.get(fucntorConfig.datasource);
		switch (streaming.type()) {
		case KAFKA:
			Map<String, Integer> topicsMap = new HashMap<>();
			final List<JavaPairRDD<String, String>> results = new ArrayList<>();
			for (String t : streaming.topics())
				topicsMap.put(t, 1);
			JavaPairReceiverInputDStream<String, String> kafka = KafkaUtils.createStream(context.ssc, ((KafkaDataSource) dsc).quonum,
					((KafkaDataSource) dsc).group, topicsMap);
			kafka.foreachRDD((Function<JavaPairRDD<String, String>, Void>) rdd -> {
				if (results.size() == 0) results.add(rdd);
				else results.set(0, results.get(0).union(rdd));
				return null;
			});
			KafkaMarshaller km = (KafkaMarshaller) dsc.marshaller;
			return results.get(0).mapToPair(t -> new Tuple2<String, F>(km.unmarshallId(t._1), km.unmarshall(t._2, functorClass)));
		default:
			throw new IllegalArgumentException("Unsupportted stocking mode: " + streaming.type());
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private FunctorConfig parseFunctorConfigs(Calculus calc, Map<Class<? extends Functor<?>>, FunctorConfig> stocking,
			Map<Class<? extends Functor<?>>, FunctorConfig> streaming) throws IOException {
		Calculating calcing = calc.getClass().getAnnotation(Calculating.class);
		FunctorConfig destConfig = parseConfig((Class<? extends Functor>) calcing.saving());
		stocking.clear();
		for (Class<? extends Functor> f : calcing.stocking()) {
			FunctorConfig c = parseConfig(f);
			if (null != c) stocking.put((Class<? extends Functor<?>>) f, c);
		}
		streaming.clear();
		for (Class<? extends Functor> f : calcing.streaming()) {
			FunctorConfig c = parseConfig(f);
			if (null != c) streaming.put((Class<? extends Functor<?>>) f, c);
		}
		return destConfig;
	}

	private <F extends Functor<F>> FunctorConfig parseConfig(Class<F> functorClass) throws IOException {
		if (null == functorClass) return null;
		Stocking stocking = functorClass.getAnnotation(Stocking.class);
		Streaming streaming = functorClass.getAnnotation(Streaming.class);
		FunctorConfig config = new FunctorConfig();
		config.datasource = stocking.source();
		config.functorClass = functorClass;
		if (!context.datasources.containsKey(stocking.source())) switch (stocking.type()) {
		case HBASE:
			if (Functor.NOT_DEFINED.equals(stocking.table()))
				throw new IllegalArgumentException("Table not defined for functor " + functorClass.toString());
			config.hbaseTable = stocking.table();
			context.datasources.get(config.datasource).marshaller = new HbaseResultMarshaller();
			break;
		case MONGODB:
			if (Functor.NOT_DEFINED.equals(stocking.table()))
				throw new IllegalArgumentException("Table not defined for functor " + functorClass.toString());
			config.mongoTable = stocking.table();
			config.mongoFilter = stocking.filter();
			break;
		case CONST:
			return null;
		default:
			throw new IllegalArgumentException("Unsupportted stocking mode: " + streaming.type());
		}
		if (context.validate) context.datasources.get(config.datasource).marshaller.confirm(functorClass, config, context);
		if (streaming != null) switch (streaming.type()) {
		case KAFKA:
			config.kafkaTopics = streaming.topics();
			context.datasources.get(config.datasource).marshaller = new KafkaMarshaller();
			break;
		default:
			throw new IllegalArgumentException("Unsupportted streaming mode: " + streaming.type());
		}
		return config;
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
		DefaultParser parser = new DefaultParser();
		Options opts = new Options();
		opts.addOption("f", "config", true, "Calculus configuration file location. Defalt calculus.properties in classpath root.");
		opts.addOption("c", "classes", true,
				"Calculus classes list to be calculated, splitted by comma. Default scan all subclasses of Calculus, with annotation \"Calculating\".");
		opts.addOption("m", "mode", true, "Calculating mode, STOCKING or STREAMING. Default STREAMING.");
		opts.addOption("h", "help", false, "Print help information like this.");

		CommandLine cmd = parser.parse(opts, args);
		if (cmd.hasOption('h')) new HelpFormatter().printHelp("java net.butfly.albacore.calculus.SparkCalculator [option]...", opts);
		return cmd;
	}

	@SuppressWarnings("deprecation")
	private void scanDatasources(String appname, Map<String, Properties> dsprops) {
		for (String dsid : dsprops.keySet()) {
			Properties dbprops = dsprops.get(dsid);
			Type type = Type.valueOf(dbprops.getProperty("type"));
			switch (type) {
			case CONST:
				ConstDataSource cst = new ConstDataSource();
				cst.values = dbprops.getProperty("values").split(",");
				context.datasources.put(dsid, cst);
				break;
			case HBASE:
				HbaseDataSource h = new HbaseDataSource();
				h.configFile = dbprops.getProperty("config", "hbase-site.xml");
				context.datasources.put(dsid, h);
				break;
			case MONGODB:
				MongoDataSource m = new MongoDataSource();
				m.uri = dbprops.getProperty("uri");
				m.authuri = dbprops.getProperty("authuri", m.uri);
				m.db = dbprops.getProperty("db");
				m.client = new MongoClient(new MongoClientURI(m.uri));
				m.mongo = m.client.getDB(m.db);
				m.jongo = new Jongo(m.mongo);
				context.datasources.put(dsid, m);
				break;
			case KAFKA:
				KafkaDataSource k = new KafkaDataSource();
				k.quonum = dbprops.getProperty("quonum");
				k.group = appname;
				context.datasources.put(dsid, k);
				break;
			default:
				logger.warn("Unsupportted type: " + type);
			}
		}
	}
}
