package net.butfly.albacore.calculus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.bson.BSONObject;
import org.jongo.MongoCollection;

import com.mongodb.hadoop.MongoInputFormat;

import net.butfly.albacore.calculus.Calculating.Mode;
import net.butfly.albacore.calculus.Functor.Stocking;
import net.butfly.albacore.calculus.Functor.Streaming;
import net.butfly.albacore.calculus.datasource.CalculatorDataSource;
import net.butfly.albacore.calculus.datasource.CalculatorDataSource.HbaseDataSource;
import net.butfly.albacore.calculus.datasource.CalculatorDataSource.KafkaDataSource;
import net.butfly.albacore.calculus.datasource.CalculatorDataSource.MongoDataSource;
import net.butfly.albacore.calculus.marshall.HbaseResultMarshaller;
import net.butfly.albacore.calculus.marshall.KafkaMarshaller;
import net.butfly.albacore.calculus.marshall.MongoMarshaller;
import scala.Tuple2;

@SuppressWarnings({ "rawtypes", "unchecked", "serial" })
public final class Calculator {
	// spark
	protected CalculatorConfig globalConfig;
	protected Map<Class<? extends Functor<?>>, FunctorConfig> stockingConfigs = new HashMap<>();
	protected Map<Class<? extends Functor<?>>, FunctorConfig> streamingConfigs = new HashMap<>();
	private FunctorConfig destConfig;

	Calculator(Calculus calc, CalculatorConfig config) throws IOException {
		this.globalConfig = config;
		Calculating calcing = calc.getClass().getAnnotation(Calculating.class);
		this.destConfig = parseConfig(calcing.saving());
		for (Class<? extends Functor<?>> f : calcing.stocking())
			this.stockingConfigs.put(f, parseConfig(f));
		for (Class<? extends Functor<?>> f : calcing.streaming())
			this.streamingConfigs.put(f, parseConfig(f));
	}

	final <F extends Functor> void calculate(Calculus calc, Mode mode) throws IOException {
		Map<Class<? extends Functor<?>>, JavaPairRDD<String, ? extends Functor<?>>> stockingFunctors = new HashMap<>();
		Map<Class<? extends Functor<?>>, JavaPairRDD<String, ? extends Functor<?>>> streamingFunctors = new HashMap<>();
		switch (mode) {
		case STOCKING:
			for (Class<? extends Functor<?>> c : this.stockingConfigs.keySet())
				stockingFunctors.put(c, (JavaPairRDD<String, ? extends Functor<?>>) stocking((Class<? extends Functor>) c,
						this.stockingConfigs.get(c)));
			break;
		case STREAMING:
			for (Class<? extends Functor<?>> c : this.stockingConfigs.keySet())
				stockingFunctors.put(c, (JavaPairRDD<String, ? extends Functor<?>>) streaming((Class<? extends Functor>) c,
						this.stockingConfigs.get(c)));
			break;
		}
		for (Class<? extends Functor<?>> c : this.streamingConfigs.keySet())
			streamingFunctors.put(c, (JavaPairRDD<String, ? extends Functor<?>>) stocking((Class<? extends Functor>) c,
					this.streamingConfigs.get(c)));
		((JavaRDD<F>) calc.calculate(globalConfig.sc, stockingFunctors, streamingFunctors)).foreach(new VoidFunction<F>() {
			@Override
			public void call(F result) throws Exception {
				MongoCollection col = ((MongoDataSource) globalConfig.datasources.get(destConfig.datasource)).jongo
						.getCollection(destConfig.mongoTable);
				col.save(result);
			}
		});
	}

	private <F extends Functor<F>> JavaPairRDD<String, F> stocking(Class<F> klass, FunctorConfig config) {
		CalculatorDataSource dsc = globalConfig.datasources.get(config.datasource);
		switch (klass.getAnnotation(Stocking.class).type()) {
		case HBASE: // TODO: adaptor to hbase data frame
			Configuration hconf = HBaseConfiguration.create();
			try {
				hconf.addResource(Thread.currentThread().getContextClassLoader()
						.getResource(((HbaseDataSource) globalConfig.datasources.get(config.datasource)).configFile)
						.openStream());
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			hconf.set(TableInputFormat.INPUT_TABLE, config.hbaseTable);
			// conf.hconf.set(TableInputFormat.SCAN_COLUMNS, "cf1:vc cf1:vs");
			final HbaseResultMarshaller hm = (HbaseResultMarshaller) dsc.marshaller;
			return this.globalConfig.sc
					.newAPIHadoopRDD(hconf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class)
					.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, F>() {
						@Override
						public Tuple2<String, F> call(Tuple2<ImmutableBytesWritable, Result> t) throws Exception {
							return new Tuple2<String, F>(hm.unmarshallId(t._1), hm.unmarshall(t._2, klass));
						}
					});
		case MONGODB:
			Configuration mconf = new Configuration();
			MongoDataSource mds = (MongoDataSource) globalConfig.datasources.get(config.datasource);
			mconf.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");
			mconf.set("mongo.auth.uri", mds.authuri);
			mconf.set("mongo.input.uri", mds.uri + "." + config.mongoTable);
			if (config.mongoFilter != null) mconf.set("mongo.input.query", config.mongoFilter);
			// conf.mconf.set("mongo.input.fields
			mconf.set("mongo.input.notimeout", "true");
			MongoMarshaller mm = (MongoMarshaller) dsc.marshaller;
			return (JavaPairRDD<String, F>) this.globalConfig.sc
					.newAPIHadoopRDD(((MongoDataSource) dsc).mconf, MongoInputFormat.class, Object.class, BSONObject.class)
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
		CalculatorDataSource dsc = globalConfig.datasources.get(config.datasource);
		switch (streaming.type()) {
		case KAFKA:
			Map<String, Integer> topicsMap = new HashMap<>();
			final List<JavaPairRDD<String, String>> results = new ArrayList<>();
			for (String t : streaming.topics())
				topicsMap.put(t, 1);
			JavaPairReceiverInputDStream<String, String> kafka = KafkaUtils.createStream(this.globalConfig.ssc,
					((KafkaDataSource) dsc).quonum, ((KafkaDataSource) dsc).group, topicsMap);
			kafka.foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {
				@Override
				public Void call(JavaPairRDD<String, String> rdd) throws Exception {
					if (results.size() == 0) results.add(rdd);
					else results.set(0, results.get(0).union(rdd));
					return null;
				}
			});
			KafkaMarshaller km = (KafkaMarshaller) dsc.marshaller;
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
		FunctorConfig config = new FunctorConfig();
		config.datasource = stocking.source();
		config.functorClass = klass;
		if (!globalConfig.datasources.containsKey(stocking.source())) switch (stocking.type()) {
		case HBASE:
			config.hbaseTable = stocking.table();
			globalConfig.datasources.get(config.datasource).marshaller = new HbaseResultMarshaller();
			break;
		case MONGODB:
			config.mongoTable = stocking.table();
			config.mongoFilter = stocking.filter();
			break;
		default:
			throw new IllegalArgumentException("Unsupportted stocking mode: " + streaming.type());
		}
		if (globalConfig.validate)
			globalConfig.datasources.get(config.datasource).marshaller.confirm(klass, config, globalConfig);
		if (streaming != null) switch (streaming.type()) {
		case KAFKA:
			config.kafkaTopics = streaming.topics();
			globalConfig.datasources.get(config.datasource).marshaller = new KafkaMarshaller();
			break;
		default:
			throw new IllegalArgumentException("Unsupportted stocking mode: " + streaming.type());
		}
		return config;
	}
}
