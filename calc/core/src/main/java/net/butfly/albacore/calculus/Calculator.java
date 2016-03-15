package net.butfly.albacore.calculus;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
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
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoClientURIBuilder;

import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import net.butfly.albacore.calculus.Calculating.Mode;
import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.datasource.DataSource.ConstDataSource;
import net.butfly.albacore.calculus.datasource.DataSource.HbaseDataSource;
import net.butfly.albacore.calculus.datasource.DataSource.KafkaDataSource;
import net.butfly.albacore.calculus.datasource.DataSource.MongoDataSource;
import net.butfly.albacore.calculus.functor.Functor;
import net.butfly.albacore.calculus.functor.FunctorConfig;
import net.butfly.albacore.calculus.functor.Functors;
import net.butfly.albacore.calculus.functor.Functor.Stocking;
import net.butfly.albacore.calculus.functor.Functor.Streaming;
import net.butfly.albacore.calculus.functor.Functor.Type;
import net.butfly.albacore.calculus.datasource.Detail;
import net.butfly.albacore.calculus.marshall.Marshaller;
import net.butfly.albacore.calculus.utils.Reflections;
import scala.Tuple2;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class Calculator implements Serializable {
	private static final long serialVersionUID = 7850755405377027618L;

	private static final Logger logger = LoggerFactory.getLogger(Calculator.class);
	private static SparkConf sconf;
	private static JavaSparkContext sc;
	private static JavaStreamingContext ssc;
	private static Map<String, DataSource> datasources = new HashMap<>();

	public boolean validate;
	private int dura;
	private Set<Class<?>> calculuses;
	private Mode mode;
	private boolean debug;

	public static void main(String... args) throws Exception {
		final Properties props = new Properties();
		CommandLine cmd = commandline(args);
		props.load(scanInputStream(cmd.getOptionValue('f', "calculus.properties")));
		for (String key : System.getProperties().stringPropertyNames())
			if (key.startsWith("calculus.")) props.put(key, System.getProperty(key));
		if (cmd.hasOption('m')) props.setProperty("calculus.mode", cmd.getOptionValue('m').toUpperCase());
		if (cmd.hasOption('c')) props.setProperty("calculus.classes", cmd.getOptionValue('c'));
		if (cmd.hasOption('d')) props.setProperty("calculus.debug", cmd.getOptionValue('d'));
		new Calculator(props).start().calculate().finish();
	}

	private Calculator start() {
		sc = new JavaSparkContext(sconf);
		if (mode == Mode.STREAMING) ssc = new JavaStreamingContext(sc, Durations.seconds(dura));
		return this;
	}

	private Calculator finish() {
		if (mode == Mode.STREAMING) {
			ssc.start();
			ssc.awaitTermination();
		}
		return this;
	}

	private Calculator(Properties props) throws Exception {
		mode = Mode.valueOf(props.getProperty("calculus.mode", "STREAMING").toUpperCase());
		debug = Boolean.valueOf(props.getProperty("calculus.debug", "false").toLowerCase());
		dura = Integer.parseInt(props.getProperty("calculus.spark.duration.seconds", "5"));
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
				Class<?> c = Class.forName(cn);
				if (Calculus.class.isAssignableFrom(c) && c.isAnnotationPresent(Calculating.class)) {
					calculuses.add(c);
					logger.debug("Found: " + c.toString());
				} else {
					logger.warn("Ignore: " + c.toString() + ", either not Calculus or not annotated by @Calculating.");
				}
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

	private <OUT extends Functor<OUT>> Calculator calculate() throws Exception {
		for (Class<?> c : calculuses) {
			logger.info("Calculus " + c.toString() + " starting... ");
			Calculus<String, OUT> calc;
			try {
				calc = (Calculus<String, OUT>) c.newInstance();
			} catch (Exception e) {
				logger.error("Calculus " + c.toString() + " constructor failure, ignored", e);
				continue;
			}
			Calculating calcing = c.getAnnotation(Calculating.class);
			FunctorConfig[] configs = scan(mode, calcing.value());
			Class<OUT> out = Reflections.resolveGenericParameter(c, Calculus.class, "OUTV");
			FunctorConfig<OUT> save = scan(Mode.STOCKING, out);

			VoidFunction<JavaPairRDD<String, OUT>> handler = null;
			if (calc != null) {
				DataSource ds = datasources.get(save.dbid);
				Configuration conf = HBaseConfiguration.create();
				final Marshaller<?, ?> m = ds.getMarshaller();
				switch (ds.getType()) {
				case HBASE:
					try {
						conf.addResource(scanInputStream(((HbaseDataSource) ds).getConfigFile()));
					} catch (IOException e) {
						throw new RuntimeException("HBase configuration invalid.", e);
					}
					conf.set(TableOutputFormat.OUTPUT_TABLE, save.detail.hbaseTable);
					handler = r -> r.mapToPair(t -> new Tuple2<>(m.marshallId(t._1), m.marshall(t._2))).saveAsNewAPIHadoopFile(null,
							ImmutableBytesWritable.class, Result.class, TableOutputFormat.class, conf);
					break;
				case MONGODB:
					MongoDataSource mds = (MongoDataSource) ds;
					conf.set("mongo.job.output.format", MongoOutputFormat.class.getName());
					MongoClientURI uri = new MongoClientURI(mds.getUri());
					conf.set("mongo.output.uri",
							new MongoClientURIBuilder(uri).collection(uri.getDatabase(), save.detail.mongoTable).build().toString());
					handler = r -> r.mapToPair(t -> new Tuple2<>(m.marshallId(t._1), m.marshall(t._2))).saveAsNewAPIHadoopFile(null,
							Object.class, BSONObject.class, MongoOutputFormat.class, conf);
					break;
				case CONSTAND_TO_CONSOLE:
					String[] values = ((ConstDataSource) ds).getValues();
					if (values == null) values = new String[0];
					handler = r -> {
						for (Tuple2<?, OUT> o : r.collect())
							logger.info("Calculated, result => " + o._2.toString());
					};
				default:
					throw new UnsupportedOperationException("Unsupportted stocking mode: " + ds.getType());
				}
			}

			switch (mode) {
			case STOCKING:
				calc.stocking(sc, read(configs), handler);
				break;
			case STREAMING:
				calc.streaming(ssc, read(configs), handler);
				break;
			}
			logger.info("Calculus " + c.toString() + " started. ");
		}
		return this;
	}

	private FunctorConfig<? extends Functor<?>>[] scan(Mode mode, Class<? extends Functor<?>>[] functors) throws IOException {
		List<FunctorConfig<?>> configs = new ArrayList<>();
		for (Class c : functors)
			configs.add(scan(mode, c));
		return configs.toArray(new FunctorConfig[configs.size()]);
	}

	private <F extends Functor<F>> FunctorConfig<F> scan(Mode mode, Class<F> functor) throws IOException {
		FunctorConfig<F> c = new FunctorConfig<F>();
		c.functorClass = functor;
		if (mode == Mode.STREAMING && functor.isAnnotationPresent(Streaming.class)) {
			c.mode = Mode.STREAMING;
			Streaming s = functor.getAnnotation(Streaming.class);
			c.dbid = s.source();
			switch (s.type()) {
			case KAFKA:
				c.detail = new Detail(s.topics());
				break;
			default:
				throw new UnsupportedOperationException("Unsupportted streaming mode: " + s.type());
			}
		} else {
			c.mode = Mode.STOCKING;
			Stocking s = functor.getAnnotation(Stocking.class);
			c.dbid = s.source();
			switch (s.type()) {
			case HBASE:
				if (Functor.NOT_DEFINED.equals(s.table()))
					throw new IllegalArgumentException("Table not defined for functor " + functor.toString());
				c.detail = new Detail(s.table());
				break;
			case MONGODB:
				if (Functor.NOT_DEFINED.equals(s.table()))
					throw new IllegalArgumentException("Table not defined for functor " + functor.toString());
				c.detail = new Detail(s.table(), Functor.NOT_DEFINED.equals(s.filter()) ? null : s.filter());
				break;
			case CONSTAND_TO_CONSOLE:
				break;
			default:
				throw new UnsupportedOperationException("Unsupportted stocking mode: " + s.type());
			}
			if (validate) {
				DataSource ds = datasources.get(s.source());
				ds.getMarshaller().confirm(functor, ds, c.detail);
			}
		}
		return c;
	}

	private Functors read(FunctorConfig<?>[] configs) throws IOException {
		Functors functors = new Functors();
		for (FunctorConfig<?> c : configs) {
			switch (mode) {
			case STOCKING:
				functors.stocking(c.functorClass, stocking((Class<? extends Functor>) c.functorClass, datasources.get(c.dbid), c.detail));
				break;
			case STREAMING:
				switch (c.mode) {
				case STOCKING:
					functors.streaming(c.functorClass, Functors.streamize(ssc,
							stocking((Class<? extends Functor>) c.functorClass, datasources.get(c.dbid), c.detail)));
					break;
				case STREAMING:
					functors.streaming(c.functorClass,
							streaming((Class<? extends Functor>) c.functorClass, datasources.get(c.dbid), c.detail));
					break;
				}
				break;
			}
		}
		return functors;
	}

	private <F extends Functor<F>> JavaPairRDD<String, F> stocking(Class<F> functor, DataSource ds, Detail detail) {
		switch (ds.getType()) {
		case HBASE: // TODO: adaptor to hbase data frame
			Configuration hconf = HBaseConfiguration.create();
			try {
				hconf.addResource(scanInputStream(((HbaseDataSource) ds).getConfigFile()));
			} catch (IOException e) {
				throw new RuntimeException("HBase configuration invalid.", e);
			}
			hconf.set(TableInputFormat.INPUT_TABLE, detail.hbaseTable);
			if (debug) {
				int limit = Integer.parseInt(System.getProperty("calculus.debug.hbase.limit", "10"));
				logger.warn("Hbase debugging, limit results in " + limit + "(can be customized by -Dcalculus.debug.hbase.limit=N)");
				try {
					hconf.set(TableInputFormat.SCAN,
							Base64.encodeBytes(ProtobufUtil.toScan(new Scan().setFilter(new PageFilter(limit))).toByteArray()));
				} catch (IOException e) {
					logger.error("Hbase debugging failure, page scan definition error", e);
				}
			}

			// conf.hconf.set(TableInputFormat.SCAN_COLUMNS, "cf1:vc cf1:vs");
			final Marshaller<Result, ImmutableBytesWritable> hm = (Marshaller<Result, ImmutableBytesWritable>) ds.getMarshaller();
			JavaPairRDD<ImmutableBytesWritable, Result> hbase = sc.newAPIHadoopRDD(hconf, TableInputFormat.class,
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
			Marshaller<BSONObject, Object> mm = (Marshaller<BSONObject, Object>) ds.getMarshaller();
			JavaPairRDD<Object, BSONObject> mongo = sc.newAPIHadoopRDD(mconf, MongoInputFormat.class, Object.class, BSONObject.class);
			return mongo.mapToPair(t -> new Tuple2<String, F>(mm.unmarshallId(t._1), mm.unmarshall(t._2, functor)));
		case CONSTAND_TO_CONSOLE:
			String[] values = ((ConstDataSource) ds).getValues();
			if (values == null) values = new String[0];
			return sc.parallelize(Arrays.asList(values))
					.mapToPair(t -> new Tuple2<String, F>(UUID.randomUUID().toString(), (F) Reflections.construct(functor, t)));
		default:
			throw new UnsupportedOperationException("Unsupportted stocking mode: " + ds.getType());
		}
	}

	private <F extends Functor<F>> JavaPairDStream<String, F> streaming(Class<F> functor, DataSource ds, Detail detail) throws IOException {
		switch (ds.getType()) {
		case KAFKA:
			Marshaller<byte[], String> km = (Marshaller<byte[], String>) ds.getMarshaller();
			JavaPairInputDStream<String, byte[]> kafka = this.kafka((KafkaDataSource) ds, functor.getAnnotation(Streaming.class).topics());
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
			return KafkaUtils.createDirectStream(ssc, String.class, byte[].class, StringDecoder.class, DefaultDecoder.class, params,
					new HashSet<String>(Arrays.asList(topics)));
		} else {
			params.put("bootstrap.servers", ds.getServers());
			params.put("auto.commit.enable", "false");
			params.put("group.id", ds.getGroup());
			Map<String, Integer> topicsMap = new HashMap<>();
			for (String t : topics)
				topicsMap.put(t, ds.getTopicPartitions());
			return KafkaUtils.createStream(ssc, String.class, byte[].class, StringDecoder.class, DefaultDecoder.class, params, topicsMap,
					StorageLevel.MEMORY_ONLY());
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
				datasources.put(dsid, new HbaseDataSource(dbprops.getProperty("config", "hbase-site.xml"), m));
				break;
			case MONGODB:
				datasources.put(dsid, new MongoDataSource(dbprops.getProperty("uri"), m));
				// , dbprops.getProperty("authdb"),dbprops.getProperty("authdb")
				break;
			case KAFKA:

				datasources.put(dsid,
						new KafkaDataSource(dbprops.getProperty("servers"), dbprops.getProperty("root"),
								Integer.parseInt(dbprops.getProperty("topic.partitions", "1")),
								debug ? appname + UUID.randomUUID().toString() : appname, m));
				break;
			default:
				logger.warn("Unsupportted type: " + type);
			}
		}
	}

	private void traceRDD(JavaRDDLike rdd, DataSource ds, Detail detail) {
		if (logger.isTraceEnabled()) logger
				.trace("{" + rdd.count() + "} Raw records from " + ds.getType() + " [" + ds.toString() + "]=>" + detail.toString() + "]. ");
	}

	public static final InputStream scanInputStream(String file) throws FileNotFoundException, IOException {
		URL url = Thread.currentThread().getContextClassLoader().getResource(file);
		return null == url ? new FileInputStream(file) : url.openStream();
	}
}
