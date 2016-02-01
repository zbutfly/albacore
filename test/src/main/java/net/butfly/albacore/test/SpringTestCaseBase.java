package net.butfly.albacore.test;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public abstract class SpringTestCaseBase {
	private ApplicationContext context;

	public SpringTestCaseBase() {
		this.initSpringContext();
	};

	protected void initSpringContext() {
		String[] conf = this.getConfiguration();
		if (null != conf && conf.length > 0) {
			this.context = new ClassPathXmlApplicationContext(this.getConfiguration());
		} else {
			this.context = null;
		}
	}

	protected Object getBean(String name) {
		return null == this.context ? null : this.context.getBean(name);
	}

	protected abstract String[] getConfiguration();
}
