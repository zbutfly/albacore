package net.butfly.albacore.base;

import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.io.Resource;

import net.butfly.albacore.utils.Springs;

public class ModuleApplicationContext extends GenericXmlApplicationContext {
	public ModuleApplicationContext(String moduleName, Class<?> relativeClass, String... resourceNames) {
		load(relativeClass, resourceNames);
		this.append(getBeanFactory(), moduleName);
		refresh();
	}

	public ModuleApplicationContext(String moduleName, Resource... resources) {
		load(resources);
		this.append(getBeanFactory(), moduleName);
		refresh();
	}

	public ModuleApplicationContext(String moduleName, String... resourceLocations) {
		load(resourceLocations);
		this.append(getBeanFactory(), moduleName);
		refresh();
	}

	private void append(ConfigurableListableBeanFactory beanFactory, String module) {
		if (null == module) return;
		while (module.startsWith("/"))
			module = module.substring(1);
		while (module.endsWith("/"))
			module = module.substring(0, module.length() - 1);
		Springs.appendPlaceholder(beanFactory, 99, Springs.searchResource(module + "-internal.properties", module + ".properties"),
				"albacore.mybatis.config.location.pattern", "classpath*:**/" + module + "-mybatis-config.xml");
	}
}
