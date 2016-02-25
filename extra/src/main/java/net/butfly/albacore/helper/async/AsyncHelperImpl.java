package net.butfly.albacore.helper.async;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.helper.AsyncHelper;
import net.butfly.albacore.helper.HelperBase;
import net.butfly.albacore.utils.Objects;
import net.butfly.albacore.utils.imports.meta.MetaObject;

import org.springframework.core.task.AsyncTaskExecutor;

public class AsyncHelperImpl extends HelperBase implements AsyncHelper {
	private static final long serialVersionUID = 6262380628765238950L;
	private long startTimeout = AsyncTaskExecutor.TIMEOUT_INDEFINITE;
	private AsyncTaskExecutor executor = null;
	private Map<String, Object> params = new HashMap<String, Object>();
	private Class<? extends AsyncTaskExecutor> executorClass;

	public Thread execute(AsyncTaskBase task) throws AsyncTaskException {
		if (null == this.executor) this.executor = this.getExecutor(this.executorClass, params);
		Thread t = new RunnableTask(task);
		this.executor.execute(t, this.startTimeout);
		return t;
	}

	public void setCorePoolSize(int corePoolSize) {
		this.params.put("corePoolSize", corePoolSize);
	}

	@SuppressWarnings("unchecked")
	public void setExecutorClassName(String className) {
		try {
			this.executorClass = (Class<? extends AsyncTaskExecutor>) Class.forName(className);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void setStartTimeout(long startTimeout) {
		this.startTimeout = startTimeout;
	}

	protected final AsyncTaskExecutor getExecutor(Class<? extends AsyncTaskExecutor> executorClass, Map<String, Object> params) {
		AsyncTaskExecutor executor;
		try {
			executor = (AsyncTaskExecutor) executorClass.newInstance();
		} catch (Exception ex) {
			throw new SystemException("ALC_000", "Unable to create instance of call executor of class: " + executorClass.getName(), ex);
		}
		MetaObject bm = Objects.createMeta(executor);
		for (String name : params.keySet())
			if (bm.hasSetter(name)) bm.setValue(name, params.get(name));

		// for ExecutorConfigurationSupport
		Method initMethod;
		try {
			initMethod = executor.getClass().getMethod("initialize");
		} catch (NoSuchMethodException e) {
			initMethod = null;
		}
		if (initMethod != null) try {
			initMethod.invoke(executor);
		} catch (Exception ex) {
			throw new SystemException("ALC_000", "Unable to initialize call executor of class: " + executorClass.getName(), ex);
		}
		return executor;
	}
}
