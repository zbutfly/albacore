package net.butfly.albacore.io.pump;

import java.util.List;

import com.google.common.base.Joiner;

import net.butfly.albacore.io.Input;
import net.butfly.albacore.io.Output;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.Reflections;

public class FanoutPump extends PumpImpl {
	private static final long serialVersionUID = -9048673309470370198L;

	public <V> FanoutPump(Input<V> input, int parallelism, List<? extends Output<V>> list) {
		super(input.name() + ">" + Joiner.on('-').join(Collections.transform(list, d -> d.name())), parallelism);
		Reflections.noneNull("Pump source/destination should not be null", input);
		Reflections.noneNull("Pump source/destination should not be null", list);
		depend(input);
		depend(list);
		pumping(() -> input.empty(), () -> {
			if (input.opened()) {
				List<V> l = input.dequeue(batchSize);
				if (l.size() > 0) {
					stats(l);
					for (Output<V> q : list)
						q.enqueue(l);
				}
			}
		});
	}
}
