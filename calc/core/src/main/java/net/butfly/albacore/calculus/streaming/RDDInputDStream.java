package net.butfly.albacore.calculus.streaming;

import org.apache.spark.api.java.function.Function0;
import org.apache.spark.rdd.EmptyRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;

import scala.Option;

class RDDInputDStream<T> extends RDDDStream<T> {
	protected Mechanism mechanism;
	protected Function0<RDD<T>> loader;

	public RDDInputDStream(StreamingContext ssc, Mechanism mechanism, Function0<RDD<T>> loader) {
		super(ssc);
		this.mechanism = mechanism;
		this.loader = loader;
		load();
	}

	@Override
	public Option<RDD<T>> compute(Time time) {
		try {
			return super.compute(time);
		} finally {
			switch (mechanism) {
			case CONST:
				break;
			case FRESH:
				load();
				break;
			case STOCK:
				current = new EmptyRDD<T>(sc, current.elementClassTag());
				break;
			default:
				throw new UnsupportedOperationException();
			}
		}
	}

	@Override
	protected RDD<T> load() {
		try {
			current = loader.call();
		} catch (Exception e) {
			throw new IllegalArgumentException(e);
		}
		if (current == null) throw new IllegalArgumentException("RDD null illegal in streamizing");
		return current;
	}
}