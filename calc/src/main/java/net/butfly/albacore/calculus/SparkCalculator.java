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
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.bson.BSONObject;
import org.jongo.MongoCollection;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.util.MongoClientURIBuilder;

import kafka.serializer.StringDecoder;
import net.butfly.albacore.calculus.Calculating.Mode;
import net.butfly.albacore.calculus.CalculatorContext.SparkCalculatorContext;
import net.butfly.albacore.calculus.Functor.Streaming;
import net.butfly.albacore.calculus.Functor.Type;
import net.butfly.albacore.calculus.FunctorConfig.Detail;
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

@SuppressWarnings({ "unchecked", "rawtypes" })
public class SparkCalculator {
	private static final Logger logger = LoggerFactory.getLogger(SparkCalculator.class);

	private SparkCalculatorContext context;
	private Set<Class<?>> calculuses;
	private Mode mode;

	public static void main(String... args) throws Exception {
		final Properties props = new Properties();
		CommandLine cmd = commandline(args);
		props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(cmd.getOptionValue('f', "calculus.properties")));
		for (String key : System.getProperties().stringPropertyNames())
			if (key.startsWith("calculus.")) props.put(key, System.getProperty(key));
		if (cmd.hasOption('m')) props.setProperty("calculus.mode", cmd.getOptionValue('m').toUpperCase());
		if (cmd.hasOption('c')) props.setProperty("calculus.classes", cmd.getOptionValue('c'));

		new SparkCalculator(props).calculate();
	}

	private SparkCalculator(Properties props) throws Exception {
		mode = Mode.valueOf(props.getProperty("calculus.mode", "STREAMING").toUpperCase());
		context = new SparkCalculatorContext();
		context.validate = Boolean.parseBoolean(props.getProperty("calculus.validate.table", "false"));
		final String appname = props.getProperty("calculus.app.name", "Calculuses");
		// dadatabse configurations parsing
		parseDatasources(appname, subprops(props, "calculus.ds."));
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

	private <F extends Functor<F>> void calculate() throws Exception {
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
					Map<Class<? extends Functor<?>>, JavaPairRDD<String, ? extends Functor<?>>> functors = fetchFunctors(scanFunctors(c));
					JavaRDD<F> r = (JavaRDD<F>) calc.calculate(context.sc, functors);
					FunctorConfig destConfig = FunctorConfig.parse((Class<? extends Functor>) c.getAnnotation(Calculating.class).saving(),
							Mode.STOCKING);
					if (destConfig != null && null != r) {
						for (String ds : destConfig.savingDSs.keySet())
							r.foreach(new WriteFunction(context.datasources.get(ds), destConfig.savingDSs.get(ds)));
					}
					return null;
				}

			}/* , new net.butfly.albacore.utils.async.Options().fork() */).execute();
			if (mode == Mode.STREAMING) {
				context.ssc.start();
				context.ssc.awaitTermination();
			}
			logger.info("Calculus " + c.toString() + " started. ");
		}
	}

	private Map<Class<? extends Functor<?>>, FunctorConfig>[] scanFunctors(Class<?> calc) throws IOException {
		Map<Class<? extends Functor<?>>, FunctorConfig> stockingConfigs = new HashMap<>();
		Map<Class<? extends Functor<?>>, FunctorConfig> streamingConfigs = new HashMap<>();

		Calculating calcing = calc.getAnnotation(Calculating.class);
		for (Class<? extends Functor> f : calcing.stocking()) {
			FunctorConfig fc = FunctorConfig.parse(f, Mode.STOCKING);
			if (null != fc) stockingConfigs.put((Class<? extends Functor<?>>) f, fc);
		}
		for (Class<? extends Functor> f : calcing.streaming()) {
			FunctorConfig fc = FunctorConfig.parse(f, mode);
			if (null != fc) streamingConfigs.put((Class<? extends Functor<?>>) f, fc);
		}
		if (context.validate) for (FunctorConfig c : stockingConfigs.values())
			c.confirm(context);

		return new Map[] { stockingConfigs, streamingConfigs };
	}

	private Map<Class<? extends Functor<?>>, JavaPairRDD<String, ? extends Functor<?>>> fetchFunctors(
			Map<Class<? extends Functor<?>>, FunctorConfig>[] configs) throws IOException {
		Map<Class<? extends Functor<?>>, JavaPairRDD<String, ? extends Functor<?>>> functors = new HashMap<>();
		// Map<Class<? extends Functor<?>>, JavaPairRDD<String, ? extends
		// Functor<?>>> stockingFunctors = new HashMap<>();
		// Map<Class<? extends Functor<?>>, JavaPairRDD<String, ? extends
		// Functor<?>>> streamingFunctors = new HashMap<>();
		for (Class<? extends Functor<?>> f : configs[0].keySet()) {
			FunctorConfig fc = configs[0].get(f);
			for (String ds : fc.stockingDSs.keySet())
				functors.put(f, (JavaPairRDD<String, ? extends Functor<?>>) stocking((Class<? extends Functor>) f,
						context.datasources.get(ds), fc.stockingDSs.get(ds)));
		}
		switch (mode) {
		case STOCKING:
			for (Class<? extends Functor<?>> f : configs[1].keySet()) {
				FunctorConfig fc = configs[1].get(f);
				for (String ds : fc.stockingDSs.keySet())
					functors.put(f, (JavaPairRDD<String, ? extends Functor<?>>) stocking((Class<? extends Functor>) f,
							context.datasources.get(ds), fc.stockingDSs.get(ds)));
			}
			break;
		case STREAMING:
			for (Class<? extends Functor<?>> f : configs[1].keySet()) {
				FunctorConfig fc = configs[1].get(f);
				for (String ds : fc.streamingDSs.keySet())
					functors.put(f, (JavaPairRDD<String, ? extends Functor<?>>) streaming((Class<? extends Functor>) f,
							context.datasources.get(ds), fc.streamingDSs.get(ds)));
			}
			break;
		}
		return functors;
	}

	private <F extends Functor<F>> JavaPairRDD<String, F> stocking(Class<F> functor, CalculatorDataSource ds, FunctorConfig.Detail detail) {
		switch (ds.getType()) {
		case HBASE: // TODO: adaptor to hbase data frame
			Configuration hconf = HBaseConfiguration.create();
			try {
				hconf.addResource(
						Thread.currentThread().getContextClassLoader().getResource(((HbaseDataSource) ds).getConfigFile()).openStream());
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			hconf.set(TableInputFormat.INPUT_TABLE, detail.hbaseTable);
			// conf.hconf.set(TableInputFormat.SCAN_COLUMNS, "cf1:vc cf1:vs");
			final HbaseResultMarshaller hm = (HbaseResultMarshaller) ds.getMarshaller();
			JavaPairRDD<ImmutableBytesWritable, Result> hbase = context.sc.newAPIHadoopRDD(hconf, TableInputFormat.class,
					ImmutableBytesWritable.class, Result.class);
			traceRDD(hbase, ds, detail);
			return hbase.mapToPair(t -> new Tuple2<String, F>(hm.unmarshallId(t._1), hm.unmarshall(t._2, functor)));
		case MONGODB:
			Configuration mconf = new Configuration();
			MongoDataSource mds = (MongoDataSource) ds;
			mconf.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");
			MongoClientURI uri = mds.getUri();
			// mconf.set("mongo.auth.uri", uri.toString());
			mconf.set("mongo.input.uri", new MongoClientURIBuilder(uri).collection(mds.getDb(), detail.mongoTable).build().toString());
			if (detail.mongoFilter != null && !"".equals(detail.mongoFilter)) mconf.set("mongo.input.query", detail.mongoFilter);
			// conf.mconf.set("mongo.input.fields
			mconf.set("mongo.input.notimeout", "true");
			MongoMarshaller mm = (MongoMarshaller) ds.getMarshaller();
			JavaPairRDD<Object, BSONObject> mongo = context.sc.newAPIHadoopRDD(mconf, MongoInputFormat.class, Object.class,
					BSONObject.class);
			return mongo.mapToPair(t -> new Tuple2<String, F>(mm.unmarshallId(t._1), mm.unmarshall(t._2, functor)));
		case CONSTAND_TO_CONSOLE:
			String[] values = ((ConstDataSource) ds).getValues();
			if (values == null) values = new String[0];
			return context.sc.parallelize(Arrays.asList(values))
					.mapToPair(t -> new Tuple2<String, F>(UUID.randomUUID().toString(), (F) Reflections.construct(functor, t)));
		default:
			throw new UnsupportedOperationException("Unsupportted stocking mode: " + ds.getType());
		}
	}

	private <F extends Functor<F>> JavaPairRDD<String, F> streaming(Class<F> functor, CalculatorDataSource ds, FunctorConfig.Detail detail)
			throws IOException {
		switch (ds.getType()) {
		case KAFKA:
			final List<JavaPairRDD<String, String>> results = new ArrayList<>();
			// KafkaUtils.createRDD(context.sc, arg1, arg2, arg3, arg4, arg5,
			// arg6, arg7, arg8, arg9);
			JavaPairInputDStream kafka = this.kafka((KafkaDataSource) ds, functor.getAnnotation(Streaming.class).topics());
			traceStream(kafka, ds, detail);
			kafka.foreachRDD((Function<JavaPairRDD<String, String>, Void>) rdd -> {
				if (results.size() == 0) results.add(rdd);
				else results.set(0, results.get(0).union(rdd));
				return null;
			});
			KafkaMarshaller km = (KafkaMarshaller) ds.getMarshaller();
			return results.size() > 0
					? results.get(0).mapToPair(t -> new Tuple2<String, F>(km.unmarshallId(t._1), km.unmarshall(t._2, functor))) : null;
		default:
			throw new UnsupportedOperationException("Unsupportted stocking mode: " + ds.getType() + " on " + functor);
		}
	}

	private JavaPairInputDStream kafka(KafkaDataSource ds, String[] topics) {
		if (ds.getRoot() == null) { // direct mode
			Map<String, String> params = new HashMap<>();
			params.put("bootstrap.servers", ds.getServers());
			// params.put("auto.commit.enable", "false");
			params.put("group.id", ds.getGroup());
			return KafkaUtils.createDirectStream(context.ssc, String.class, String.class, StringDecoder.class, StringDecoder.class, params,
					new HashSet<String>(Arrays.asList(topics)));
		} else {
			Map<String, Integer> topicsMap = new HashMap<>();
			for (String t : topics)
				topicsMap.put(t, ds.getTopicPartitions());
			return KafkaUtils.createStream(context.ssc, ds.getServers(), ds.getGroup(), topicsMap);
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

	private void parseDatasources(String appname, Map<String, Properties> dsprops) {
		for (String dsid : dsprops.keySet()) {
			Properties dbprops = dsprops.get(dsid);
			Type type = Type.valueOf(dbprops.getProperty("type"));
			switch (type) {
			case CONSTAND_TO_CONSOLE:
				context.datasources.put(dsid, new ConstDataSource(dbprops.getProperty("values").split(",")));
				break;
			case HBASE:
				context.datasources.put(dsid, new HbaseDataSource(dbprops.getProperty("config", "hbase-site.xml")));
				break;
			case MONGODB:
				context.datasources.put(dsid, new MongoDataSource(dbprops
						.getProperty("uri")/*
											 * , dbprops.getProperty("authdb"),
											 * dbprops.getProperty("authdb")
											 */));
				break;
			case KAFKA:
				context.datasources.put(dsid, new KafkaDataSource(dbprops.getProperty("servers"), dbprops.getProperty("root"),
						Integer.parseInt(dbprops.getProperty("topic.partitions", "1")), appname));
				break;
			default:
				logger.warn("Unsupportted type: " + type);
			}
		}
	}

	@SuppressWarnings("serial")
	private void traceStream(JavaDStreamLike stream, CalculatorDataSource ds, Detail detail) {
		if (!logger.isTraceEnabled()) return;
		long[] i = new long[] { 0 };
		JavaDStream<Long> s = stream.count();
		s.foreachRDD(new Function<JavaRDD<Long>, Void>() {
			@Override
			public Void call(JavaRDD<Long> rdd) throws Exception {
				i[0] += rdd.reduce(new Function2<Long, Long, Long>() {
					@Override
					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
				});
				return null;
			};
		});
		logger.trace("{" + i[0] + "} Raw records from " + ds.getType() + " [" + ds.toString() + "]=>" + detail.toString() + "].");

	}

	private void traceRDD(JavaRDDLike rdd, CalculatorDataSource ds, Detail detail) {
		if (!logger.isTraceEnabled()) return;
		logger.trace("{" + rdd.count() + "} Raw records from " + ds.getType() + " [" + ds.toString() + "]=>" + detail.toString() + "]. ");

	}

	private static class WriteFunction<F extends Functor<F>> implements VoidFunction<F> {
		private static final long serialVersionUID = -3114062446562954303L;
		private CalculatorDataSource datasource;
		private Detail detail;

		public WriteFunction(CalculatorDataSource dataSource, Detail detail) {
			this.datasource = dataSource;
			this.detail = detail;
		}

		@Override
		public void call(F result) throws Exception {
			switch (datasource.getType()) {
			case MONGODB:
				MongoCollection col = ((MongoDataSource) datasource).getJongo().getCollection(detail.mongoTable);
				col.save(result);
				break;
			case CONSTAND_TO_CONSOLE:
				logger.info("Result: " + result.toString());
				break;
			default:
				throw new UnsupportedOperationException("Write to " + datasource.getType() + " is not supported.");
			}
		}
	}
}
