package net.butfly.albacore.calculus.datasource;

import java.util.HashMap;

import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.elasticsearch.spark.rdd.EsSpark;

import com.google.common.base.CaseFormat;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.FactroingConfig;
import net.butfly.albacore.calculus.factor.filter.FactorFilter;
import net.butfly.albacore.calculus.factor.modifier.PrimaryKey;
import net.butfly.albacore.calculus.factor.rds.PairRDS;
import net.butfly.albacore.calculus.factor.rds.internal.RDSupport;
import net.butfly.albacore.calculus.factor.rds.internal.WrappedRDD;
import net.butfly.albacore.calculus.lambda.Func;
import net.butfly.albacore.calculus.marshall.Marshaller;
import net.butfly.albacore.calculus.utils.Reflections;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Map;

@SuppressWarnings("rawtypes")
public class ElasticDataSource extends DataSource<String, String, Map, String, Object> {
	private static final long serialVersionUID = 5990012673598725014L;
	public final String baseUrl;

	public static final class M extends Marshaller<String, String, Map> {
		private static final long serialVersionUID = -3615969198441078477L;

		public M() {
			super();
		}

		public M(Func<String, String> mapping) {
			super(mapping);
		}
	};

	public ElasticDataSource(String baseUrl, boolean validate, CaseFormat srcf, CaseFormat dstf) {
		super(Type.ELASTIC, null, validate, M.class, String.class, Map.class, OutputFormat.class, null, srcf, dstf);
		this.baseUrl = baseUrl;
	}

	@Override
	public <V> Tuple2<String, Object> beforeWriting(String key, V value) {
		if (null == value) return null;
		Reflections.set(value, Marshaller.parseFirstOfAny(value.getClass(), PrimaryKey.class)._1, key);
		return new Tuple2<>(null, value);
	}

	@Override
	public void save(JavaPairRDD<String, Object> rdd, FactroingConfig<?> dd) {
		java.util.Map<String, String> m = new HashMap<>();
		m.put("es.mapping.id", marshaller.parseQualifier(Marshaller.parseFirstOfAny(dd.factorClass, PrimaryKey.class)._1));
		EsSpark.saveToEs(rdd.values().rdd(), dd.table, JavaConverters.asScalaMapConverter(m).asScala());
	}

	@Override
	public <F extends Factor<F>> PairRDS<String, F> stocking(Class<F> factor, FactroingConfig<F> detail, float expandPartitions,
			FactorFilter... filters) {
		JavaPairRDD<String, Map<String, Object>> records = JavaPairRDD.fromRDD(EsSpark.esRDD(Calculator.calculator.sc.sc(), baseUrl
				+ detail.table, filter(detail.query, filters)), RDSupport.tag(), RDSupport.tag());
		if (expandPartitions > 1) records = records.repartition((int) Math.ceil(records.getNumPartitions() * expandPartitions));
		JavaPairRDD<String, F> r = records.mapToPair((Tuple2<String, Map<String, Object>> t) -> new Tuple2<>(marshaller.unmarshallId(t._1),
				marshaller.unmarshall(t._2, factor)));
		return new PairRDS<>(new WrappedRDD<>(r));
	}

	private String filter(String filter, FactorFilter[] filters) {
		return null;
	}

	@Override
	public String andQuery(String... ands) {
		throw new UnsupportedOperationException();
	}
}