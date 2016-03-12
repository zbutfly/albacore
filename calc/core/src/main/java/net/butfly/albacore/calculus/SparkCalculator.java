package net.butfly.albacore.calculus;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
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
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.bson.BSONObject;
import org.jongo.MongoCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.util.MongoClientURIBuilder;

import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import net.butfly.albacore.calculus.Calculating.Mode;
import net.butfly.albacore.calculus.CalculatorContext.StockingContext;
import net.butfly.albacore.calculus.CalculatorContext.StreamingContext;
import net.butfly.albacore.calculus.Functor.Streaming;
import net.butfly.albacore.calculus.Functor.Type;
import net.butfly.albacore.calculus.FunctorConfig.Detail;
import net.butfly.albacore.calculus.datasource.DataContext;
import net.butfly.albacore.calculus.datasource.DataContext.MongoContext;
import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.datasource.DataSource.ConstDataSource;
import net.butfly.albacore.calculus.datasource.DataSource.HbaseDataSource;
import net.butfly.albacore.calculus.datasource.DataSource.KafkaDataSource;
import net.butfly.albacore.calculus.datasource.DataSource.MongoDataSource;
import net.butfly.albacore.calculus.marshall.HbaseHiveMarshaller;
import net.butfly.albacore.calculus.marshall.KafkaMarshaller;
import net.butfly.albacore.calculus.marshall.MongoMarshaller;
import net.butfly.albacore.calculus.utils.Reflections;
import scala.Tuple2;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class SparkCalculator implements Serializable {
	private static final long serialVersionUID = 7850755405377027618L;

	private static final Logger logger = LoggerFactory.getLogger(SparkCalculator.class);

	private StockingContext stockingContext;
	private StreamingContext streamingContext;
	private int streamingDuration;
	private Set<Class<?>> calculuses;
	private Mode mode;

	public static void main(String... args) throws Exception {
		final Properties props = new Properties();
		CommandLine cmd = commandline(args);
		props.load(scanInputStream(cmd.getOptionValue('f', "calculus.properties")));
		for (String key : System.getProperties().stringPropertyNames())
			if (key.startsWith("calculus.")) props.put(key, System.getProperty(key));
		if (cmd.hasOption('m')) props.setProperty("calculus.mode", cmd.getOptionValue('m').toUpperCase());
		if (cmd.hasOption('c')) props.setProperty("calculus.classes", cmd.getOptionValue('c'));

		new SparkCalculator(props).calculate();
	}

	private SparkCalculator(Properties props) throws Exception {
		mode = Mode.valueOf(props.getProperty("calculus.mode", "STREAMING").toUpperCase());
		streamingDuration = Integer.parseInt(props.getProperty("calculus.spark.duration.seconds", "5"));
		stockingContext = new StockingContext();
		stockingContext.validate = Boolean.parseBoolean(props.getProperty("calculus.validate.table", "false"));
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
		stockingContext.sc = new JavaSparkContext(sconf);
		// scan and run calculuses
		Set<String> names = new HashSet<>();
		if (props.containsKey("calculus.classes")) names.addAll(Arrays.asList(props.getProperty("calculus.classes").split(",")));

		calculuses = new HashSet<>();
		FastClasspathScanner scaner = props.containsKey("calculus.package") ? new FastClasspathScanner()
				: new FastClasspathScanner(props.getProperty("calculus.package").split(","));
		scaner.matchClassesWithAnnotation(Calculating.class, c -> {}).matchClassesImplementing(Calculus.class, c -> {
			if (!names.isEmpty() && names.contains(c)) calculuses.add(c);
		}).scan();
	}

	private <F extends Functor<F>> void calculate() throws Exception {
		if (mode == Mode.STREAMING) streamingContext = new StreamingContext(stockingContext, streamingDuration);
		for (Class<?> c : calculuses) {
			logger.info("Calculus " + c.toString() + " starting... ");
			Calculus calc;
			try {
				calc = (Calculus) c.newInstance();
			} catch (Exception e) {
				logger.error("Calculus " + c.toString() + " constructor failure, ignored", e);
				continue;
			}
			Map<Class<? extends Functor<?>>, JavaPairRDD<String, ? extends Functor<?>>> functors = fetchFunctors(scanFunctors(c));
			JavaRDD<F> r = (JavaRDD<F>) calc.calculate(stockingContext.sc, functors);
			FunctorConfig destConfig = FunctorConfig.parse((Class<? extends Functor>) c.getAnnotation(Calculating.class).saving(),
					Mode.STOCKING);
			if (destConfig != null && null != r) {
				for (String ds : destConfig.savingDSs.keySet())
					r.foreach(new WriteFunction(stockingContext.datasources.get(ds), destConfig.savingDSs.get(ds)));
			}
			if (mode == Mode.STREAMING) {
				streamingContext.ssc.start();
				streamingContext.ssc.awaitTermination();
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
		if (stockingContext.validate) for (FunctorConfig c : stockingConfigs.values())
			c.confirm(stockingContext);

		return new Map[] { stockingConfigs, streamingConfigs };
	}

	private Map<Class<? extends Functor<?>>, JavaPairRDD<String, ? extends Functor<?>>> fetchFunctors(
			Map<Class<? extends Functor<?>>, FunctorConfig>[] configs) throws IOException {
		Map<Class<? extends Functor<?>>, JavaPairRDD<String, ? extends Functor<?>>> functors = new HashMap<>();
		for (Class<? extends Functor<?>> f : configs[0].keySet()) {
			FunctorConfig fc = configs[0].get(f);
			for (String ds : fc.stockingDSs.keySet())
				functors.put(f, (JavaPairRDD<String, ? extends Functor<?>>) stocking((Class<? extends Functor>) f,
						stockingContext.datasources.get(ds), fc.stockingDSs.get(ds)));
		}
		switch (mode) {
		case STOCKING:
			for (Class<? extends Functor<?>> f : configs[1].keySet()) {
				FunctorConfig fc = configs[1].get(f);
				for (String ds : fc.stockingDSs.keySet())
					functors.put(f, (JavaPairRDD<String, ? extends Functor<?>>) stocking((Class<? extends Functor>) f,
							stockingContext.datasources.get(ds), fc.stockingDSs.get(ds)));
			}
			break;
		case STREAMING:
			for (Class<? extends Functor<?>> f : configs[1].keySet()) {
				FunctorConfig fc = configs[1].get(f);
				for (String ds : fc.streamingDSs.keySet()) {
					JavaPairDStream<String, ? extends Functor> dstr = streaming((Class<? extends Functor>) f,
							stockingContext.datasources.get(ds), fc.streamingDSs.get(ds));
					functors.put(f, (JavaPairRDD<String, ? extends Functor<?>>) Calculus.union(dstr));
				}
			}
			break;
		}
		return functors;
	}

	private <F extends Functor<F>> JavaPairRDD<String, F> stocking(Class<F> functor, DataSource ds, FunctorConfig.Detail detail) {
		switch (ds.getType()) {
		case HBASE: // TODO: adaptor to hbase data frame
			Configuration hconf = HBaseConfiguration.create();
			try {
				hconf.addResource(scanInputStream(((HbaseDataSource) ds).getConfigFile()));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			hconf.set(TableInputFormat.INPUT_TABLE, detail.hbaseTable);
			// conf.hconf.set(TableInputFormat.SCAN_COLUMNS, "cf1:vc cf1:vs");
			final HbaseHiveMarshaller hm = (HbaseHiveMarshaller) ds.getMarshaller();
			JavaPairRDD<ImmutableBytesWritable, Result> hbase = stockingContext.sc.newAPIHadoopRDD(hconf, TableInputFormat.class,
					ImmutableBytesWritable.class, Result.class);
			traceRDD(hbase, ds, detail);
			return hbase.mapToPair(t -> new Tuple2<String, F>(hm.unmarshallId(t._1), hm.unmarshall(t._2, functor)));
		case MONGODB:
			Configuration mconf = new Configuration();
			MongoDataSource mds = (MongoDataSource) ds;
			mconf.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");
			MongoClientURI uri = new MongoClientURI(mds.getUri());
			// mconf.set("mongo.auth.uri", uri.toString());
			mconf.set("mongo.input.uri",
					new MongoClientURIBuilder(uri).collection(uri.getDatabase(), detail.mongoTable).build().toString());
			if (detail.mongoFilter != null && !"".equals(detail.mongoFilter)) mconf.set("mongo.input.query", detail.mongoFilter);
			// conf.mconf.set("mongo.input.fields
			mconf.set("mongo.input.notimeout", "true");
			MongoMarshaller mm = (MongoMarshaller) ds.getMarshaller();
			JavaPairRDD<Object, BSONObject> mongo = stockingContext.sc.newAPIHadoopRDD(mconf, MongoInputFormat.class, Object.class,
					BSONObject.class);
			return mongo.mapToPair(t -> new Tuple2<String, F>(mm.unmarshallId(t._1), mm.unmarshall(t._2, functor)));
		case CONSTAND_TO_CONSOLE:
			String[] values = ((ConstDataSource) ds).getValues();
			if (values == null) values = new String[0];
			return stockingContext.sc.parallelize(Arrays.asList(values))
					.mapToPair(t -> new Tuple2<String, F>(UUID.randomUUID().toString(), (F) Reflections.construct(functor, t)));
		default:
			throw new UnsupportedOperationException("Unsupportted stocking mode: " + ds.getType());
		}
	}

	private <F extends Functor<F>> JavaPairDStream<String, F> streaming(Class<F> functor, DataSource ds, FunctorConfig.Detail detail)
			throws IOException {
		switch (ds.getType()) {
		case KAFKA:
			JavaPairInputDStream<String, byte[]> kafka = this.kafka((KafkaDataSource) ds, functor.getAnnotation(Streaming.class).topics());
			traceStream(kafka, ds, detail);
			KafkaMarshaller km = (KafkaMarshaller) ds.getMarshaller();
			return kafka.mapToPair(t -> new Tuple2<String, F>(km.unmarshallId(t._1), km.unmarshall(t._2, functor)));
		default:
			throw new UnsupportedOperationException("Unsupportted stocking mode: " + ds.getType() + " on " + functor);
		}
	}

	private JavaPairInputDStream<String, byte[]> kafka(KafkaDataSource ds, String[] topics) {
		Map<String, String> params = new HashMap<>();
		if (ds.getRoot() == null) { // direct mode
			params.put("metadata.broker.list", ds.getServers());
			// params.put("bootstrap.servers", ds.getServers());
			// params.put("auto.commit.enable", "false");
			params.put("group.id", ds.getGroup());
			return KafkaUtils.createDirectStream(streamingContext.ssc, String.class, byte[].class, StringDecoder.class,
					DefaultDecoder.class, params, new HashSet<String>(Arrays.asList(topics)));
		} else {
			params.put("bootstrap.servers", ds.getServers());
			params.put("auto.commit.enable", "false");
			params.put("group.id", ds.getGroup());
			Map<String, Integer> topicsMap = new HashMap<>();
			for (String t : topics)
				topicsMap.put(t, ds.getTopicPartitions());
			return KafkaUtils.createStream(streamingContext.ssc, String.class, byte[].class, StringDecoder.class, DefaultDecoder.class,
					params, topicsMap, StorageLevel.MEMORY_ONLY());
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
				stockingContext.datasources.put(dsid, new ConstDataSource(dbprops.getProperty("values").split(",")));
				break;
			case HBASE:
				stockingContext.datasources.put(dsid, new HbaseDataSource(dbprops.getProperty("config", "hbase-site.xml")));
				break;
			case MONGODB:
				stockingContext.datasources.put(dsid, new MongoDataSource(dbprops
						.getProperty("uri")/*
											 * , dbprops.getProperty("authdb"),
											 * dbprops.getProperty("authdb")
											 */));
				break;
			case KAFKA:
				stockingContext.datasources.put(dsid, new KafkaDataSource(dbprops.getProperty("servers"), dbprops.getProperty("root"),
						Integer.parseInt(dbprops.getProperty("topic.partitions", "1")), appname));
				break;
			default:
				logger.warn("Unsupportted type: " + type);
			}
		}
	}

	private void traceStream(JavaDStreamLike stream, DataSource ds, Detail detail) {
		if (!logger.isTraceEnabled()) return;
		long[] i = new long[] { 0 };
		((JavaDStream<Long>) stream.count()).foreachRDD(rdd -> {
			i[0] += rdd.reduce((c1, c2) -> c1 + c2);
		});
		logger.trace("{" + i[0] + "} Raw records from " + ds.getType() + " [" + ds.toString() + "]=>" + detail.toString() + "].");

	}

	private void traceRDD(JavaRDDLike rdd, DataSource ds, Detail detail) {
		if (!logger.isTraceEnabled()) return;
		logger.trace("{" + rdd.count() + "} Raw records from " + ds.getType() + " [" + ds.toString() + "]=>" + detail.toString() + "]. ");

	}

	private static class WriteFunction<F extends Functor<F>> implements VoidFunction<F> {
		private static final long serialVersionUID = -3114062446562954303L;
		private DataSource datasource;
		private DataContext datacontext;
		private Detail detail;

		public WriteFunction(DataSource ds, Detail detail) {
			this.datasource = ds;
			this.detail = detail;
			switch (ds.getType()) {
			case MONGODB:
				this.datacontext = new MongoContext(ds);
				break;
			case CONSTAND_TO_CONSOLE:
				break;
			default:
				throw new UnsupportedOperationException("Write to " + datasource.getType() + " is not supported.");
			}
		}

		@Override
		public void call(F result) throws Exception {
			switch (datasource.getType()) {
			case MONGODB:
				MongoContext dc = (MongoContext) this.datacontext;
				MongoCollection col = dc.getJongo().getCollection(detail.mongoTable);
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

	public static final InputStream scanInputStream(String file) throws FileNotFoundException, IOException {
		URL url = Thread.currentThread().getContextClassLoader().getResource(file);
		return null == url ? new FileInputStream(file) : url.openStream();
	}
}
