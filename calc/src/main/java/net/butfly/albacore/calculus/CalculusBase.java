package net.butfly.albacore.calculus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.hadoop.MongoInputFormat;

import net.butfly.albacore.calculus.Calculus.Mode;
import net.butfly.albacore.calculus.Functor.Stocking;
import net.butfly.albacore.calculus.Functor.Streaming;
import net.butfly.albacore.calculus.marshall.HbaseResultMarshaller;
import net.butfly.albacore.calculus.marshall.KafkaMarshaller;
import net.butfly.albacore.calculus.marshall.MongodbMarshaller;
import scala.Tuple2;

@SuppressWarnings({ "rawtypes", "unchecked", "serial" })
public abstract class CalculusBase {
	protected Logger logger = LoggerFactory.getLogger(this.getClass());
	// spark
	protected CalculatorConfig globalConfig;
	protected Map<Class<? extends Functor>, FunctorConfig> stockingConfigs = new HashMap<>();
	protected Map<Class<? extends Functor>, FunctorConfig> streamingConfigs = new HashMap<>();
	private FunctorConfig destConfig;

	protected CalculusBase() {}

	public CalculusBase(CalculatorConfig config) throws IOException {
		Class<? extends CalculusBase> c = this.getClass();
		Calculus calc = c.getAnnotation(Calculus.class);
		for (Class<? extends Functor> f : calc.stocking())
			this.stockingConfigs.put(f, parseConfig(f));
		for (Class<? extends Functor> f : calc.streaming())
			this.streamingConfigs.put(f, parseConfig(f));
		this.destConfig = parseConfig(calc.saving());
	}

	abstract public JavaRDD<? extends Functor> calculate(Map<Class<? extends Functor>, JavaPairRDD<String, ? extends Functor>> stocking,
			Map<Class<? extends Functor>, JavaPairRDD<String, ? extends Functor>> streaming);

	final public <F extends Functor> void calculate(Mode mode) throws IOException {
		Map<Class<? extends Functor>, JavaPairRDD<String, ? extends Functor>> stockingFunctors = new HashMap<>();
		Map<Class<? extends Functor>, JavaPairRDD<String, ? extends Functor>> streamingFunctors = new HashMap<>();
		switch (mode) {
		case STOCKING:
			for (Class<? extends Functor> c : this.stockingConfigs.keySet())
				stockingFunctors.put(c, stocking((Class<? extends Functor>) c, this.stockingConfigs.get(c)));
			break;
		case STREAMING:
			for (Class<? extends Functor> c : this.stockingConfigs.keySet())
				stockingFunctors.put(c, streaming((Class<? extends Functor>) c, this.stockingConfigs.get(c)));
			break;
		}
		for (Class<? extends Functor> c : this.streamingConfigs.keySet())
			streamingFunctors.put(c, stocking((Class<? extends Functor>) c, this.streamingConfigs.get(c)));
		((JavaRDD<F>) this.calculate(stockingFunctors, streamingFunctors)).foreach(new VoidFunction<F>() {
			@Override
			public void call(F result) throws Exception {
				destConfig.mcol.save(result);
			}
		});
	}

	final public JavaSparkContext getSparkContext() {
		return this.globalConfig.sc;
	}

	private <F extends Functor<F>> JavaPairRDD<String, F> stocking(Class<F> klass, FunctorConfig config) {
		switch (klass.getAnnotation(Stocking.class).type()) {
		case HBASE: // TODO: adaptor to hbase data frame
			HbaseResultMarshaller hm = (HbaseResultMarshaller) config.marshaller;
			return this.globalConfig.sc.newAPIHadoopRDD(config.hconf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class)
					.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, F>() {
						@Override
						public Tuple2<String, F> call(Tuple2<ImmutableBytesWritable, Result> t) throws Exception {
							return new Tuple2<String, F>(hm.unmarshallId(t._1), hm.unmarshall(t._2, klass));
						}
					});
		case MONGODB:
			MongodbMarshaller mm = (MongodbMarshaller) config.marshaller;
			return (JavaPairRDD<String, F>) this.globalConfig.sc
					.newAPIHadoopRDD(config.mconf, MongoInputFormat.class, Object.class, BSONObject.class)
					.mapToPair(new PairFunction<Tuple2<Object, BSONObject>, String, F>() {
						@Override
						public Tuple2<String, F> call(Tuple2<Object, BSONObject> t) throws Exception {
							return new Tuple2<String, F>(mm.unmarshallId(t._1), mm.unmarshall(t._2, klass));
						}
					});
		default:
			throw new IllegalArgumentException("Unsupportted stocking mode: " + klass.getAnnotation(Stocking.class).type());
		}
	}

	private <F extends Functor<F>> JavaPairRDD<String, F> streaming(Class<F> klass, FunctorConfig config) throws IOException {
		Streaming streaming = klass.getAnnotation(Streaming.class);
		String src = streaming.source();
		switch (streaming.type()) {
		case KAFKA:
			Map<String, Integer> topicsMap = new HashMap<>();
			final List<JavaPairRDD<String, String>> results = new ArrayList<>();
			for (String t : streaming.topics())
				topicsMap.put(t, 1);
			JavaPairReceiverInputDStream<String, String> kafka = KafkaUtils.createStream(this.globalConfig.ssc,
					globalConfig.kafkas.get(src).quonum, globalConfig.kafkas.get(src).group, topicsMap);
			kafka.foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {
				@Override
				public Void call(JavaPairRDD<String, String> rdd) throws Exception {
					if (results.size() == 0) results.add(rdd);
					else results.set(0, results.get(0).union(rdd));
					return null;
				}
			});
			KafkaMarshaller km = (KafkaMarshaller) config.marshaller;
			return results.get(0).mapToPair(new PairFunction<Tuple2<String, String>, String, F>() {
				@Override
				public Tuple2<String, F> call(Tuple2<String, String> t) throws Exception {
					return new Tuple2<String, F>(km.unmarshallId(t._1), km.unmarshall(t._2, klass));
				}
			});
		default:
			throw new IllegalArgumentException("Unsupportted stocking mode: " + streaming.type());
		}
	}

	private <F extends Functor> FunctorConfig parseConfig(Class<F> klass) throws IOException {
		if (null == klass) return null;
		Stocking stocking = klass.getAnnotation(Stocking.class);
		Streaming streaming = klass.getAnnotation(Streaming.class);
		FunctorConfig conf = new FunctorConfig();
		conf.datasource = stocking.source();
		conf.functorClass = klass;
		switch (stocking.type()) {
		case HBASE:
			conf.hconf = HBaseConfiguration.create();
			conf.hconf.addResource(Thread.currentThread().getContextClassLoader()
					.getResource(globalConfig.hbases.get(conf.datasource).config).openStream());
			conf.htname = TableName.valueOf(stocking.table());
			// TODO confirm/create table.
			// Admin ha = conf.hconn.getAdmin();
			// TODO confirm/insert data into table.
			// Table ht = conf.hconn.getTable(conf.htname);
			conf.hconf.set(TableInputFormat.INPUT_TABLE, stocking.table());
			// conf.hconf.set(TableInputFormat.SCAN_COLUMNS, "cf1:vc cf1:vs");
			// schema for data frame
			conf.marshaller = new HbaseResultMarshaller();
			break;
		case MONGODB:
			conf.mconf = new Configuration();
			conf.mconf.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");
			conf.mconf.set("mongo.auth.uri", globalConfig.mongodbs.get(conf.datasource).authuri);
			conf.mconf.set("mongo.input.uri", globalConfig.mongodbs.get(conf.datasource).uri + "." + stocking.table());
			conf.mconf.set("mongo.input.query", stocking.filter());
			// conf.mconf.set("mongo.input.fields
			conf.mconf.set("mongo.input.notimeout", "true");
			conf.mcol = globalConfig.mongodbs.get(destConfig.datasource).jongo.getCollection(stocking.table());
			conf.marshaller = new MongodbMarshaller();
			break;
		default:
			throw new IllegalArgumentException("Unsupportted stocking mode: " + streaming.type());
		}
		conf.marshaller.confirm(klass, conf, globalConfig);
		switch (streaming.type()) {
		case KAFKA:
			conf.kafkaTopics = streaming.topics();
			conf.marshaller = new KafkaMarshaller();
			break;
		default:
			throw new IllegalArgumentException("Unsupportted stocking mode: " + streaming.type());
		}
		return conf;
	}
}
