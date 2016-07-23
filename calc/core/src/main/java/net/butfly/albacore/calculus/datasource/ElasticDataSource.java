package net.butfly.albacore.calculus.datasource;

import java.lang.reflect.Field;
import java.util.HashMap;

import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.elasticsearch.spark.rdd.EsSpark;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factor.Type;
import net.butfly.albacore.calculus.factor.filter.FactorFilter;
import net.butfly.albacore.calculus.factor.modifier.Id;
import net.butfly.albacore.calculus.factor.rds.PairRDS;
import net.butfly.albacore.calculus.factor.rds.internal.RDSupport;
import net.butfly.albacore.calculus.factor.rds.internal.WrappedRDD;
import net.butfly.albacore.calculus.marshall.Marshaller;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Map;

@SuppressWarnings("rawtypes")
public class ElasticDataSource extends DataSource<String, String, Map, String, Object> {
	private static final long serialVersionUID = 5990012673598725014L;
	public final String baseUrl;

	public ElasticDataSource(String baseUrl, boolean validate) {
		super(Type.ELASTIC, validate, new Marshaller<String, String, Map>(), String.class, Map.class, OutputFormat.class, null);
		this.baseUrl = baseUrl;
	}

	@Override
	public <V> Tuple2<String, Object> beforeWriting(String key, V value) {
		if (null == value) return null;
		@SuppressWarnings("unchecked")
		Field f = marshaller.parse(value.getClass(), Id.class)._1();
		if (null != f) {
			f.setAccessible(true);
			try {
				f.set(value, key);
			} catch (IllegalArgumentException | IllegalAccessException e) {
				error(() -> "Error in id setting", e);
			}
		}
		return new Tuple2<>(null, value);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void save(JavaPairRDD<String, Object> rdd, DataDetail<?> dd) {
		java.util.Map<String, String> m = new HashMap<>();
		m.put("es.mapping.id", marshaller.parseQualifier(marshaller.parse(dd.factorClass, Id.class)._1));
		EsSpark.saveToEs(rdd.values().rdd(), dd.tables[0], JavaConverters.asScalaMapConverter(m).asScala());
	}

	@Override
	public <F extends Factor<F>> PairRDS<String, F> stocking(Calculator calc, Class<F> factor, DataDetail<F> detail,
			float expandPartitions, FactorFilter... filters) {
		JavaPairRDD<String, Map<String, Object>> records = JavaPairRDD.fromRDD(
				EsSpark.esRDD(calc.sc.sc(), baseUrl + detail.tables[0], filter(detail.filter, filters)), RDSupport.tag(), RDSupport.tag());
		if (expandPartitions > 1) records = records.repartition((int) Math.ceil(records.getNumPartitions() * expandPartitions));
		JavaPairRDD<String, F> r = records.mapToPair((Tuple2<String, Map<String, Object>> t) -> new Tuple2<>(marshaller.unmarshallId(t._1),
				marshaller.unmarshall(t._2, factor)));
		return new PairRDS<>(new WrappedRDD<>(r));
	}

	private String filter(String filter, FactorFilter[] filters) {
		return null;
	}
}