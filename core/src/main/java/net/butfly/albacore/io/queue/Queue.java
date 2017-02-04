package net.butfly.albacore.io.queue;

import java.util.List;

import net.butfly.albacore.io.Input;
import net.butfly.albacore.io.InputThenHandler;
import net.butfly.albacore.io.InputThensHandler;
import net.butfly.albacore.io.Output;
import net.butfly.albacore.io.OutputPriorHandler;
import net.butfly.albacore.io.OutputPriorsHandler;
import net.butfly.albacore.lambda.Converter;

/**
 * Rich feature queue for big data processing, supporting:
 * <ul>
 * <li>Blocking based on capacity</li>
 * <li>Batching</li>
 * <ul>
 * <li>Batching in restrict synchronous or not</li>
 * </ul>
 * <li>Storage/pooling policies</li>
 * <ul>
 * <li>Instant</li>
 * <li>Memory (heap)</li>
 * <li>Local disk (off heap based on memory mapping), like {@link MapDB}/
 * {@link BigQueue} and so on</li>
 * <li>Remote, like Kafka/MQ and so on (Not now)</li>
 * </ul>
 * <li>Continuous or not</li>
 * <li>Connect to another ("then op", into engine named "Pump")</li>
 * <ul>
 * <li>Fan out to others ("thens op", to {@link KeyQueue})</li>
 * <li>Merge into {@link KeyQueue}</li>
 * </ul>
 * <li>Statistic</li>
 * </ul>
 * 
 * @author butfly
 *
 */
public interface Queue<I, O> extends Input<O>, Output<I> {
	static final long INFINITE_SIZE = -1;

	@Override
	long size();
	/* from interfaces */

	@Override
	default <O1> Queue<I, O1> then(Converter<O, O1> conv) {
		return new InputThenHandler<>(this, conv).proxy(Queue.class);
	}

	@Override
	default <O1> Queue<I, O1> thens(Converter<List<O>, List<O1>> conv, int parallelism) {
		return new InputThensHandler<>(this, conv, parallelism).proxy(Queue.class);
	}

	@Override
	default <I0> Queue<I0, O> prior(Converter<I0, I> conv) {
		return new OutputPriorHandler<>(this, conv).proxy(Queue.class);
	}

	@Override
	default <I0> Queue<I0, O> priors(Converter<List<I0>, List<I>> conv, int parallelism) {
		return new OutputPriorsHandler<>(this, conv, parallelism).proxy(Queue.class);
	}
}
