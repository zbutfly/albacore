// package net.butfly.albacore.calculus.factor.gds;
//
// import java.util.List;
//
// import org.apache.spark.api.java.JavaRDD;
// import org.apache.spark.api.java.function.Function;
// import org.apache.spark.api.java.function.Function2;
// import org.apache.spark.api.java.function.PairFunction;
// import org.apache.spark.api.java.function.VoidFunction;
// import org.apache.spark.graphx.Graph;
// import org.apache.spark.rdd.RDD;
// import org.apache.spark.storage.StorageLevel;
// import org.apache.spark.streaming.StreamingContext;
// import org.apache.spark.streaming.dstream.DStream;
//
// import net.butfly.albacore.calculus.factor.rds.internal.Wrapped;
// import scala.Tuple2;
//
// @Deprecated
// public class GDD<V, E> implements Wrapped<Tuple2<List<V>, List<E>>> {
// private static final long serialVersionUID = 6429769333769758509L;
// protected Graph<V, E> graph;
//
// @Override
// public void foreachRDD(VoidFunction<JavaRDD<Tuple2<List<V>, List<E>>>>
// consumer) {
// // TODO Auto-generated method stub
//
// }
//
// @Override
// public void foreach(VoidFunction<Tuple2<List<V>, List<E>>> consumer) {
// // TODO Auto-generated method stub
//
// }
//
// @Override
// public Tuple2<List<V>, List<E>> reduce(Function2<Tuple2<List<V>, List<E>>,
// Tuple2<List<V>, List<E>>, Tuple2<List<V>, List<E>>> func) {
// // TODO Auto-generated method stub
// return null;
// }
//
// @Override
// public DStream<Tuple2<List<V>, List<E>>> dstream(StreamingContext ssc) {
// // TODO Auto-generated method stub
// return null;
// }
//
// @Override
// public RDD<Tuple2<List<V>, List<E>>> rdd() {
// // TODO Auto-generated method stub
// return null;
// }
//
// @Override
// public Wrapped<Tuple2<List<V>, List<E>>> repartition(float ratio) {
// // TODO Auto-generated method stub
// return null;
// }
//
// @Override
// public Wrapped<Tuple2<List<V>, List<E>>> unpersist() {
// // TODO Auto-generated method stub
// return null;
// }
//
// @Override
// public Wrapped<Tuple2<List<V>, List<E>>> persist(StorageLevel level) {
// // TODO Auto-generated method stub
// if (null == level || StorageLevel.NONE().equals(level)) return this;
// return null;
// }
//
// @Override
// public Wrapped<Tuple2<List<V>, List<E>>> union(Wrapped<Tuple2<List<V>,
// List<E>>> other) {
// // TODO Auto-generated method stub
// return null;
// }
//
// @Override
// public Wrapped<Tuple2<List<V>, List<E>>> filter(Function<Tuple2<List<V>,
// List<E>>, Boolean> func) {
// // TODO Auto-generated method stub
// return null;
// }
//
// @Override
// public <K2, V2> Wrapped<Tuple2<K2, V2>>
// mapToPair(PairFunction<Tuple2<List<V>, List<E>>, K2, V2> func) {
// // TODO Auto-generated method stub
// return null;
// }
//
// @Override
// public <T1> Wrapped<T1> map(Function<Tuple2<List<V>, List<E>>, T1> func) {
// // TODO Auto-generated method stub
// return null;
// }
//
// @Override
// public <S> Wrapped<Tuple2<List<V>, List<E>>> sortBy(Function<Tuple2<List<V>,
// List<E>>, S> comp) {
// // TODO Auto-generated method stub
// return null;
// }
//
// @Override
// public Wrapped<Tuple2<List<V>, List<E>>> wrapped() {
// return this;
// }
// }
