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

import net.butfly.albacore.calculus.Calculating.Mode;
import net.butfly.albacore.calculus.CalculatorContext.SparkCalculatorContext;
import net.butfly.albacore.calculus.Functor.Stocking;
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
		mode = Mode.valueOf(props.getProperty("calculus.mode", "STREAMING"));
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
					Map<Class<? extends Functor<?>>, JavaPairRDD<String, ? extends Functor<?>>>[] functors = scanCalcFunctors(
							scanCalcConfigs(c));
					JavaRDD<F> r = (JavaRDD<F>) calc.calculate(context.sc, functors[0], functors[1]);
					FunctorConfig destConfig = parseConfig((Class<? extends Functor>) c.getAnnotation(Calculating.class).saving());
					if (destConfig != null && null != r) {
						for (String ds : destConfig.savingDSs.keySet())
							r.foreach(new WriteFunction(context.datasources.get(ds), destConfig.savingDSs.get(ds)));
					}
					return null;
				}

			}/* , new net.butfly.albacore.utils.async.Options().fork() */).execute();
			logger.info("Calculus " + c.toString() + " started. ");
		}
	}

	protected Map<Class<? extends Functor<?>>, JavaPairRDD<String, ? extends Functor<?>>>[] scanCalcFunctors(
			Map<Class<? extends Functor<?>>, FunctorConfig>[] configs) throws IOException {
		Map<Class<? extends Functor<?>>, JavaPairRDD<String, ? extends Functor<?>>> stockingFunctors = new HashMap<>();
		Map<Class<? extends Functor<?>>, JavaPairRDD<String, ? extends Functor<?>>> streamingFunctors = new HashMap<>();
		for (Class<? extends Functor<?>> f : configs[0].keySet()) {
			FunctorConfig fc = configs[0].get(f);
			for (String ds : fc.stockingDSs.keySet())
				stockingFunctors.put(f, (JavaPairRDD<String, ? extends Functor<?>>) stocking((Class<? extends Functor>) f,
						context.datasources.get(ds), fc.stockingDSs.get(ds)));
		}
		switch (mode) {
		case STOCKING:
			for (Class<? extends Functor<?>> f : configs[1].keySet()) {
				FunctorConfig fc = configs[1].get(f);
				for (String ds : fc.stockingDSs.keySet())
					stockingFunctors.put(f, (JavaPairRDD<String, ? extends Functor<?>>) stocking((Class<? extends Functor>) f,
							context.datasources.get(ds), fc.stockingDSs.get(ds)));
			}
			break;
		case STREAMING:
			for (Class<? extends Functor<?>> f : configs[1].keySet()) {
				FunctorConfig fc = configs[1].get(f);
				for (String ds : fc.streamingDSs.keySet())
					streamingFunctors.put(f, (JavaPairRDD<String, ? extends Functor<?>>) streaming((Class<? extends Functor>) f,
							context.datasources.get(ds), fc.streamingDSs.get(ds)));
			}
			break;
		}
		return new Map[] { stockingFunctors, streamingFunctors };
	}

	private Map<Class<? extends Functor<?>>, FunctorConfig>[] scanCalcConfigs(Class<?> calc) throws IOException {
		Map<Class<? extends Functor<?>>, FunctorConfig> stockingConfigs = new HashMap<>();
		Map<Class<? extends Functor<?>>, FunctorConfig> streamingConfigs = new HashMap<>();

		Calculating calcing = calc.getAnnotation(Calculating.class);
		for (Class<? extends Functor> f : calcing.stocking()) {
			FunctorConfig fc = parseConfig(f);
			if (null != fc) stockingConfigs.put((Class<? extends Functor<?>>) f, fc);
		}
		for (Class<? extends Functor> f : calcing.streaming()) {
			FunctorConfig fc = parseConfig(f);
			if (null != fc) streamingConfigs.put((Class<? extends Functor<?>>) f, fc);
		}
		return new Map[] { stockingConfigs, streamingConfigs };
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
			return context.sc.newAPIHadoopRDD(hconf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class)
					.mapToPair(t -> new Tuple2<String, F>(hm.unmarshallId(t._1), hm.unmarshall(t._2, functor)));
		case MONGODB:
			Configuration mconf = new Configuration();
			MongoDataSource mds = (MongoDataSource) ds;
			mconf.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");
			mconf.set("mongo.auth.uri", mds.getAuthuri());
			mconf.set("mongo.input.uri", new MongoClientURIBuilder(new MongoClientURI(mds.getUri()))
					.collection(mds.getDb(), detail.mongoTable).build().toString());
			if (detail.mongoFilter != null && !"".equals(detail.mongoFilter)) mconf.set("mongo.input.query", detail.mongoFilter);
			// conf.mconf.set("mongo.input.fields
			mconf.set("mongo.input.notimeout", "true");
			MongoMarshaller mm = (MongoMarshaller) ds.getMarshaller();
			return (JavaPairRDD<String, F>) context.sc.newAPIHadoopRDD(mconf, MongoInputFormat.class, Object.class, BSONObject.class)
					.mapToPair(t -> new Tuple2<String, F>(mm.unmarshallId(t._1), mm.unmarshall(t._2, functor)));
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
			Map<String, Integer> topicsMap = new HashMap<>();
			final List<JavaPairRDD<String, String>> results = new ArrayList<>();
			for (String t : functor.getAnnotation(Streaming.class).topics())
				topicsMap.put(t, 1);
			JavaPairReceiverInputDStream<String, String> kafka = KafkaUtils.createStream(context.ssc, ((KafkaDataSource) ds).getQuonum(),
					((KafkaDataSource) ds).getGroup(), topicsMap);
			kafka.foreachRDD((Function<JavaPairRDD<String, String>, Void>) rdd -> {
				if (results.size() == 0) results.add(rdd);
				else results.set(0, results.get(0).union(rdd));
				return null;
			});
			KafkaMarshaller km = (KafkaMarshaller) ds.getMarshaller();
			return results.get(0).mapToPair(t -> new Tuple2<String, F>(km.unmarshallId(t._1), km.unmarshall(t._2, functor)));
		default:
			throw new UnsupportedOperationException("Unsupportted stocking mode: " + ds.getType() + " on " + functor);
		}
	}

	private <F extends Functor<F>> FunctorConfig parseConfig(Class<F> functor) throws IOException {
		if (null == functor) return null;
		Streaming streaming = functor.getAnnotation(Streaming.class);
		FunctorConfig config = new FunctorConfig();
		config.functorClass = functor;
		switch (mode) {
		case STOCKING:
			config.stockingDSs.put(functor.getAnnotation(Stocking.class).source(), parseStockingConfig(functor));
			break;
		case STREAMING:
			config.stockingDSs.put(functor.getAnnotation(Stocking.class).source(), parseStockingConfig(functor));
			if (streaming != null) config.streamingDSs.put(streaming.source(), parseStreamingConfig(streaming));
			break;
		}
		return config;
	}

	private <F extends Functor<F>> Detail parseStreamingConfig(Streaming streaming) {
		if (streaming == null) return null;
		switch (streaming.type()) {
		case KAFKA:
			return new Detail(streaming.topics());
		default:
			throw new UnsupportedOperationException("Unsupportted streaming mode: " + streaming.type());
		}
	}

	private <F extends Functor<F>> Detail parseStockingConfig(Class<F> functor) {
		Stocking stocking = functor.getAnnotation(Stocking.class);
		if (Functor.NOT_DEFINED.equals(stocking.source())) return null;
		Detail detail = null;
		switch (stocking.type()) {
		case HBASE:
			if (Functor.NOT_DEFINED.equals(stocking.table()))
				throw new IllegalArgumentException("Table not defined for functor " + functor.toString());
			detail = new Detail(stocking.table());
			break;
		case MONGODB:
			if (Functor.NOT_DEFINED.equals(stocking.table()))
				throw new IllegalArgumentException("Table not defined for functor " + functor.toString());
			detail = new Detail(stocking.table(), Functor.NOT_DEFINED.equals(stocking.filter()) ? null : stocking.filter());
			break;
		case CONSTAND_TO_CONSOLE:
			break;
		default:
			throw new UnsupportedOperationException("Unsupportted stocking mode: " + stocking.type());
		}
		if (context.validate) {
			CalculatorDataSource ds = context.datasources.get(stocking.source());
			ds.getMarshaller().confirm(functor, ds, detail, context);
		}
		return detail;
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

	private void scanDatasources(String appname, Map<String, Properties> dsprops) {
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
				context.datasources.put(dsid,
						new MongoDataSource(dbprops.getProperty("uri"), dbprops.getProperty("uri.auth", dbprops.getProperty("uri"))));
				break;
			case KAFKA:
				context.datasources.put(dsid, new KafkaDataSource(dbprops.getProperty("quonum"), appname));
				break;
			default:
				logger.warn("Unsupportted type: " + type);
			}
		}
	}
}
