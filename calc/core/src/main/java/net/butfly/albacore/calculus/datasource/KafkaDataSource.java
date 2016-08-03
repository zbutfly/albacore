package net.butfly.albacore.calculus.datasource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.base.CaseFormat;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.FactroingConfig;
import net.butfly.albacore.calculus.factor.filter.FactorFilter;
import net.butfly.albacore.calculus.marshall.KafkaMarshaller;
import scala.Tuple2;

public class KafkaDataSource extends DataSource<String, String, byte[], Void, Void> {
	private static final long serialVersionUID = 7500441385655250814L;
	public String servers;
	public String group;
	public int topicPartitions;

	public KafkaDataSource(String servers, String root, int topicPartitions, String group, CaseFormat srcf, CaseFormat dstf) {
		super(Type.KAFKA, parseSchema(root, servers), false, KafkaMarshaller.class, String.class, byte[].class, NullOutputFormat.class,
				null, srcf, dstf);
		int i = servers.indexOf('/');
		if (i > 0) this.servers = servers.substring(0, servers.indexOf('/'));
		this.group = group;
		this.topicPartitions = topicPartitions;
	}

	public static String parseSchema(String rootInConf, String serversInConf) {
		int pos = serversInConf.indexOf('/');
		if (rootInConf != null) return rootInConf;
		else if (pos > 0) return serversInConf.substring(pos + 1);
		else return null;
	}

	@Override
	public String toString() {
		return super.toString() + ":" + this.servers + (null == this.schema ? "" : "/" + this.schema) + " (by " + group + ")";
	}

	@Override
	public <F extends Factor<F>> JavaPairDStream<String, F> streaming(Class<F> factor, FactroingConfig<F> detail, FactorFilter... filters) {
		debug(() -> "Streaming begin: " + factor.toString());
		JavaPairDStream<String, byte[]> kafka;
		Map<String, String> params = new HashMap<>();
		if (schema == null) { // direct mode
			params.put("metadata.broker.list", servers);
			// params.put("bootstrap.servers", ks.getServers());
			// params.put("auto.commit.enable", "false");
			params.put("group.id", group);
			kafka = KafkaUtils.createDirectStream(Calculator.calculator.ssc, String.class, byte[].class, StringDecoder.class,
					DefaultDecoder.class, params, new HashSet<String>(Arrays.asList(detail.table)));
		} else {
			params.put("bootstrap.servers", servers);
			params.put("auto.commit.enable", "false");
			params.put("group.id", group);
			Map<String, Integer> topicsMap = new HashMap<>();
			for (String t : detail.table.split(","))
				topicsMap.put(t, topicPartitions);
			kafka = KafkaUtils.createStream(Calculator.calculator.ssc, String.class, byte[].class, StringDecoder.class,
					DefaultDecoder.class, params, topicsMap, StorageLevel.MEMORY_ONLY());
		}
		return kafka.mapToPair((final Tuple2<String, byte[]> t) -> new Tuple2<String, F>(marshaller.unmarshallId(t._1), marshaller
				.unmarshall(t._2, factor)));
	}

	@Override
	public String andQuery(String... ands) {
		throw new UnsupportedOperationException();
	}
}