package net.butfly.albacore.lambda;

import java.io.Serializable;

@FunctionalInterface
public interface Runnable extends java.lang.Runnable, RunnableEx, Serializable {
	@Override
	void run();

	default Runnable prior(Runnable prior) {
		return () -> {
			prior.run();
			run();
		};
	}

	default Runnable then(Runnable then) {
		return () -> {
			run();
			then.run();
		};
	}

	default Runnable until(Supplier<Boolean> stopping) {
		return () -> {
			while (!stopping.get())
				this.run();
		};
	}

	static Runnable until(Supplier<Boolean> stopping, Runnable... then) {
		return () -> {
			while (!stopping.get())
				for (Runnable t : then)
					t.run();
		};
	}

	public static void r(RunnableEx r) {
		r.runEx();
	}
}
