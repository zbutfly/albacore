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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.bson.BSONObject;

import com.mongodb.hadoop.MongoInputFormat;

import net.butfly.albacore.calculus.marshall.HbaseFrameMarshaller;
import net.butfly.albacore.calculus.marshall.HbaseResultMarshaller;
import net.butfly.albacore.calculus.marshall.MongodbMarshaller;
import scala.Tuple2;

@SuppressWarnings("serial")
public abstract class CalculusBase {
	// spark
	protected EngineConfig engineConfig;
	protected Map<Class<? extends Functor<?>>, CalculusConfig> configs;

	protected CalculusBase() {}

	public CalculusBase(EngineConfig econf) throws IOException {
		this.configs = new HashMap<>();
		Class<? extends CalculusBase> c = this.getClass();
		for (Class<? extends Functor<?>> cc : c.getAnnotation(Calculus.class).value()) {
			Stocking stocking = c.getAnnotation(Stocking.class);
			Streaming streaming = c.getAnnotation(Streaming.class);
			CalculusConfig conf = new CalculusConfig();
			switch (stocking.type()) {
			case HBASE:
				conf.hconf = HBaseConfiguration.create();
				conf.hconf.addResource(Thread.currentThread().getContextClassLoader().getResource(econf.hconfig).openStream());
				conf.htname = TableName.valueOf(stocking.table());
				// TODO confirm/create table.
				// Admin ha = conf.hconn.getAdmin();
				// TODO confirm/insert data into table.
				// Table ht = conf.hconn.getTable(conf.htname);
				conf.hconf.set(TableInputFormat.INPUT_TABLE, stocking.table());
				// column family
				conf.hconf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "cf1");
				// 3 column
				conf.hconf.set(TableInputFormat.SCAN_COLUMNS, "cf1:vc cf1:vs");
				// schema for data frame
				List<StructField> fields = new ArrayList<StructField>();
				fields.add(DataTypes.createStructField("line", DataTypes.StringType, true));
				conf.schema = DataTypes.createStructType(fields);
				conf.marshaller = new HbaseResultMarshaller();
				break;
			case MONGODB:
				conf.mconf = new Configuration();
				conf.mconf.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");
				conf.mconf.set("mongo.input.uri", "mongodb://hzga5:30012/xddb." + stocking.table());
				conf.marshaller = new MongodbMarshaller();
				break;
			}
			switch (streaming.value()) {
			case KAFKA:
				conf.kafkaTopics = streaming.topics();
				break;
			}
			this.configs.put(cc, conf);
		}
	}

	public <F extends Functor<F>> void test(JavaPairRDD<ImmutableBytesWritable, Result> hbases, Class<Functor<F>> c) {
		HbaseFrameMarshaller m = (HbaseFrameMarshaller) configs.get(c).marshaller;
		engineConfig.sqsc.createDataFrame(hbases.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Row>() {
			@Override
			public Row call(Tuple2<ImmutableBytesWritable, Result> row) throws Exception {
				return RowFactory.create(row._1, row._2);
			}
		}), configs.get(c).schema).javaRDD().foreachAsync(new VoidFunction<Row>() {
			@Override
			public void call(Row row) throws Exception {
				F f = (F) m.unmarshall(row, c);
			}
		});
	}

	public List<Tuple2<Class<? extends Functor<?>>, JavaPairRDD<?, ?>>> stocking() throws IOException {
		List<Tuple2<Class<? extends Functor<?>>, JavaPairRDD<?, ?>>> rdds = new ArrayList<>();
		for (Class<? extends Functor<?>> c : this.configs.keySet()) {
			Stocking stocking = c.getAnnotation(Stocking.class);
			CalculusConfig conf = this.configs.get(c);
			switch (stocking.type()) {
			case HBASE:
				JavaPairRDD<ImmutableBytesWritable, Result> rdd = this.engineConfig.sc.newAPIHadoopRDD(conf.hconf, TableInputFormat.class,
						ImmutableBytesWritable.class, Result.class);
				rdds.add(new Tuple2<Class<? extends Functor<?>>, JavaPairRDD<?, ?>>(c, rdd));
				break;
			case MONGODB:
				JavaPairRDD<Object, BSONObject> docs = this.engineConfig.sc.newAPIHadoopRDD(conf.mconf, MongoInputFormat.class,
						Object.class, BSONObject.class);
				rdds.add(new Tuple2<Class<? extends Functor<?>>, JavaPairRDD<?, ?>>(c, docs));
				break;
			}
		}
		return rdds;
	}

	public List<Tuple2<Class<? extends Functor<?>>, JavaPairRDD<?, ?>>> streaming() throws IOException {
		List<Tuple2<Class<? extends Functor<?>>, JavaPairRDD<?, ?>>> rdds = new ArrayList<>();
		for (Class<? extends Functor<?>> c : this.configs.keySet()) {
			Streaming streaming = c.getAnnotation(Streaming.class);
			CalculusConfig conf = this.configs.get(c);
			switch (streaming.value()) {
			case KAFKA:
				Map<String, Integer> topicsMap = new HashMap<>();
				for (String t : streaming.topics())
					topicsMap.put(t, 1);
				KafkaUtils.createStream(this.engineConfig.ssc, engineConfig.kquonum, engineConfig.kgroup, topicsMap)
						.foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {
							@Override
							public Void call(JavaPairRDD<String, String> rdd) throws Exception {
								rdds.add(new Tuple2<Class<? extends Functor<?>>, JavaPairRDD<?, ?>>(c, rdd));
								return null;
							}
						});
				break;
			}
		}
		return rdds;
	}

	abstract public void calculate(Map<Class<? extends Functor<?>>, Object> functors);
}
