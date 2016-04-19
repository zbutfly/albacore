package net.butfly.albacore.calculus.datasource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factor.Type;
import net.butfly.albacore.calculus.factor.filter.FactorFilter;
import net.butfly.albacore.calculus.marshall.KafkaMarshaller;
import scala.Tuple2;

public class KafkaDataSource extends DataSource<String, String, byte[], Void, Void> {
	private static final long serialVersionUID = 7500441385655250814L;
	String servers;
	String root;
	String group;
	int topicPartitions;

	public KafkaDataSource(String servers, String root, int topicPartitions, String group, KafkaMarshaller marshaller) {
		// TODO
		super(Type.KAFKA, false, null == marshaller ? new KafkaMarshaller() : marshaller, String.class, byte[].class,
				NullOutputFormat.class);
		int pos = servers.indexOf('/');
		if (root == null && pos >= 0) {
			this.servers = servers.substring(0, pos);
			this.root = servers.substring(pos + 1);
		} else {
			this.root = root;
			this.servers = servers;
		}
		this.group = group;
		this.topicPartitions = topicPartitions;
	}

	public KafkaDataSource(String servers, String root, int topicPartitions, String group) {
		this(servers, root, topicPartitions, group, new KafkaMarshaller());
	}

	@Override
	public String toString() {
		return super.toString() + ":" + this.getServers() + "(" + group + ")";
	}

	public String getGroup() {
		return group;
	}

	public String getRoot() {
		return root;
	}

	public String getServers() {
		return this.servers + (null == this.root ? "" : "/" + this.root);
	}

	public int getTopicPartitions() {
		return topicPartitions;
	}

	@Override
	public <F extends Factor<F>> JavaPairDStream<String, F> streaming(Calculator calc, Class<F> factor, DataDetail<F> detail,
			FactorFilter... filters) {
		debug(() -> "Streaming begin: " + factor.toString());
		JavaPairDStream<String, byte[]> kafka;
		Map<String, String> params = new HashMap<>();
		if (root == null) { // direct mode
			params.put("metadata.broker.list", servers);
			// params.put("bootstrap.servers", ks.getServers());
			// params.put("auto.commit.enable", "false");
			params.put("group.id", group);
			kafka = KafkaUtils.createDirectStream(calc.ssc, String.class, byte[].class, StringDecoder.class, DefaultDecoder.class, params,
					new HashSet<String>(Arrays.asList(detail.tables)));
		} else {
			params.put("bootstrap.servers", servers);
			params.put("auto.commit.enable", "false");
			params.put("group.id", group);
			Map<String, Integer> topicsMap = new HashMap<>();
			for (String t : detail.tables)
				topicsMap.put(t, topicPartitions);
			kafka = KafkaUtils.createStream(calc.ssc, String.class, byte[].class, StringDecoder.class, DefaultDecoder.class, params,
					topicsMap, StorageLevel.MEMORY_ONLY());
		}
		return kafka.mapToPair((final Tuple2<String, byte[]> t) -> new Tuple2<String, F>(marshaller.unmarshallId(t._1),
				marshaller.unmarshall(t._2, factor)));
	}
}