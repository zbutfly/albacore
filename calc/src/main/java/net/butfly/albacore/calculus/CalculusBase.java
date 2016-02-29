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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.bson.BSONObject;

import com.mongodb.hadoop.MongoInputFormat;

import net.butfly.albacore.calculus.Calculus.Mode;
import net.butfly.albacore.calculus.Functor.Stocking;
import net.butfly.albacore.calculus.Functor.Streaming;
import net.butfly.albacore.calculus.marshall.HbaseResultMarshaller;
import net.butfly.albacore.calculus.marshall.MongodbMarshaller;
import scala.Tuple2;

@SuppressWarnings("serial")
public abstract class CalculusBase {
	// spark
	protected CalculatorConfig globalConfig;
	protected Map<Class<? extends Functor<?>>, FunctorConfig> stockingFunctorConfigs = new HashMap<>();
	protected Map<Class<? extends Functor<?>>, FunctorConfig> streamingFunctorConfigs = new HashMap<>();
	private FunctorConfig destConfig;
	private Class<? extends Functor<?>> destFunctor;

	protected CalculusBase() {}

	public CalculusBase(CalculatorConfig econf) throws IOException {
		Class<? extends CalculusBase> c = this.getClass();
		Calculus calc = c.getAnnotation(Calculus.class);
		for (Class<? extends Functor<?>> f : calc.stocking())
			this.stockingFunctorConfigs.put(f, parseConfig(f));
		for (Class<? extends Functor<?>> f : calc.streaming())
			this.streamingFunctorConfigs.put(f, parseConfig(f));
		this.destFunctor = calc.saving();
		this.destConfig = parseConfig(this.destFunctor);
	}

	abstract public JavaPairRDD<?, ?> calculate(Map<Class<? extends Functor<?>>, JavaPairRDD<?, ?>> stockingFunctors,
			Map<Class<? extends Functor<?>>, JavaPairRDD<?, ?>> streamingFunctors);

	@SuppressWarnings({ "rawtypes", "unchecked" })
	final public void calculate(Mode mode) throws IOException {
		Map<Class<? extends Functor<?>>, JavaPairRDD<?, ?>> stockingFunctors = new HashMap<>();
		Map<Class<? extends Functor<?>>, JavaPairRDD<?, ?>> streamingFunctors = new HashMap<>();
		switch (mode) {
		case STOCKING:
			for (Class<? extends Functor<?>> c : this.stockingFunctorConfigs.keySet())
				stockingFunctors.put(c, stocking(c, this.stockingFunctorConfigs.get(c)));
			break;
		case STREAMING:
			for (Class<? extends Functor<?>> c : this.stockingFunctorConfigs.keySet())
				stockingFunctors.put(c, streaming(c, this.stockingFunctorConfigs.get(c)));
			break;
		}
		for (Class<? extends Functor<?>> c : this.streamingFunctorConfigs.keySet())
			streamingFunctors.put(c, stocking(c, this.streamingFunctorConfigs.get(c)));
		JavaPairRDD r = this.calculate(stockingFunctors, streamingFunctors);
		r.foreach(new VoidFunction<Tuple2>() {
			@Override
			public void call(Tuple2 t) throws Exception {
				write(t._1, (BSONObject) t._2, destConfig);
			}
		});
	}

	final public JavaSparkContext getSparkContext() {
		return this.globalConfig.sc;
	}

	private void write(Object key, BSONObject value, FunctorConfig functorConfig) {
		// Only support write to mongodb.
		functorConfig.mcol.save(value);
	}

	private JavaPairRDD<?, ?> stocking(Class<? extends Functor<?>> functor, FunctorConfig functorConfig) {
		switch (functor.getAnnotation(Stocking.class).type()) {
		case HBASE: // TODO: adaptor to hbase data frame
			return this.globalConfig.sc.newAPIHadoopRDD(functorConfig.hconf, TableInputFormat.class,
					ImmutableBytesWritable.class, Result.class);
		case MONGODB:
			return this.globalConfig.sc.newAPIHadoopRDD(functorConfig.mconf, MongoInputFormat.class, Object.class,
					BSONObject.class);
		default:
			throw new IllegalArgumentException("Unsupportted stocking mode: " + functor.getAnnotation(Stocking.class).type());
		}
	}

	private JavaPairRDD<?, ?> streaming(Class<? extends Functor<?>> functorClass, FunctorConfig functorConfig)
			throws IOException {
		Streaming streaming = functorClass.getAnnotation(Streaming.class);
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
			return results.get(0);
		default:
			throw new IllegalArgumentException("Unsupportted stocking mode: " + streaming.type());
		}
	}

	private FunctorConfig parseConfig(Class<? extends Functor<?>> f) throws IOException {
		Stocking stocking = f.getAnnotation(Stocking.class);
		Streaming streaming = f.getAnnotation(Streaming.class);
		FunctorConfig conf = new FunctorConfig();
		conf.source = stocking.source();
		conf.functorClass = f;
		switch (stocking.type()) {
		case HBASE:
			conf.hconf = HBaseConfiguration.create();
			conf.hconf.addResource(Thread.currentThread().getContextClassLoader()
					.getResource(globalConfig.hbases.get(conf.source).config).openStream());
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
			conf.mconf.set("mongo.auth.uri", globalConfig.mongodbs.get(conf.source).authuri);
			conf.mconf.set("mongo.input.uri", globalConfig.mongodbs.get(conf.source).uri + "." + stocking.table());
			conf.mconf.set("mongo.input.query", stocking.filter());
			// conf.mconf.set("mongo.input.fields
			conf.mconf.set("mongo.input.notimeout", "true");
			conf.mcol = globalConfig.mongodbs.get(destConfig.source).jongo.getCollection(stocking.table());
			conf.marshaller = new MongodbMarshaller();
			break;
		default:
			throw new IllegalArgumentException("Unsupportted stocking mode: " + streaming.type());
		}
		switch (streaming.type()) {
		case KAFKA:
			conf.kafkaTopics = streaming.topics();
			break;
		default:
			throw new IllegalArgumentException("Unsupportted stocking mode: " + streaming.type());
		}
		return conf;
	}
}
