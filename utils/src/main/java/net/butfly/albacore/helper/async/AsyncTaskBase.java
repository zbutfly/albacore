package net.butfly.albacore.helper.async;

import java.io.Serializable;

public abstract class AsyncTaskBase implements Serializable {

	private static final long serialVersionUID = 9142539240192398052L;
	private String name;

	public AsyncTaskBase(String name) {
		this.name = name;
	}

	public final String getName() {
		return this.name;
	}

	public abstract Object execute();

	public abstract void callback(Object result);

	public abstract void exception(Thread thread, Throwable exception);
}
