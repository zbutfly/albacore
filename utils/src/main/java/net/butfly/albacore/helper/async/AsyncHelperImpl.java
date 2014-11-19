package net.butfly.albacore.helper.async;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.beanutils.BeanMap;
import org.springframework.core.task.AsyncTaskExecutor;

import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.helper.AsyncHelper;
import net.butfly.albacore.helper.HelperBase;

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

	@SuppressWarnings({ "unchecked" })
	protected final AsyncTaskExecutor getExecutor(Class<? extends AsyncTaskExecutor> executorClass, Map<String, Object> params) {
		AsyncTaskExecutor executor;
		try {
			executor = (AsyncTaskExecutor) executorClass.newInstance();
		} catch (Exception ex) {
			throw new SystemException("ALC_000", "Unable to create instance of task executor of class: "
					+ executorClass.getName(), ex);
		}
		BeanMap bm = new BeanMap(executor);
		bm.putAll(params);
		// for ExecutorConfigurationSupport
		Method initMethod;
		try {
			initMethod = executor.getClass().getMethod("initialize");
		} catch (NoSuchMethodException e) {
			initMethod = null;
		}
		if (initMethod != null)
			try {
				initMethod.invoke(executor);
			} catch (Exception ex) {
				throw new SystemException("ALC_000", "Unable to initialize task executor of class: " + executorClass.getName(),
						ex);
			}
		return executor;
	}
}
