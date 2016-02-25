package net.butfly.albacore.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.core.Ordered;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;

public final class Springs extends Utils {
	private static final Logger logger = LoggerFactory.getLogger(Springs.class);

	public static void appendPlaceholderByProperties(ConfigurableBeanFactory beanFactory, int order, String... propertyNameAndValue) {
		beanFactory.registerSingleton(Keys.key(String.class), buildPlaceholder(order, propertyNameAndValue));
	}

	public static void appendPlaceholder(ConfigurableBeanFactory beanFactory, int order, Resource[] propertiesFiles,
			String... propertyNameAndValue) {
		PropertyPlaceholderConfigurer bean = buildPlaceholder(order, propertyNameAndValue);
		if (!Objects.isEmpty(propertiesFiles)) bean.setLocations(propertiesFiles);
		beanFactory.registerSingleton(Keys.key(String.class), bean);
	}

	private static PropertyPlaceholderConfigurer buildPlaceholder(int order, String... propertyNameAndValue) {
		PropertyPlaceholderConfigurer bean = new PropertyPlaceholderConfigurer();
		bean.setOrder(order < 0 ? Ordered.HIGHEST_PRECEDENCE : order);
		bean.setIgnoreResourceNotFound(true);
		bean.setIgnoreUnresolvablePlaceholders(true);

		Properties props = new Properties();
		if (null != propertyNameAndValue) for (int i = 0; i < propertyNameAndValue.length; i += 2)
			props.setProperty(propertyNameAndValue[i], propertyNameAndValue[i + 1]);
		if (!props.isEmpty()) bean.setProperties(props);
		return bean;
	}

	public static Resource searchResource(String pattern) {
		ResourcePatternResolver patternResolver = new PathMatchingResourcePatternResolver();
		Resource[] reses;
		try {
			reses = patternResolver.getResources(pattern);
		} catch (IOException e) {
			return null;
		}
		if (null == reses || reses.length == 0) {
			logger.warn("No " + pattern + " found in spring classpath.");
			return null;
		}
		if (reses.length > 1) logger
				.warn("More than one " + pattern + " found in spring classpath, the first one \"" + reses[0].toString() + "\" is loaded.");
		return reses[0];
	}

	public static Resource[] searchResource(String... pattern) {
		List<Resource> reses = new ArrayList<Resource>();
		for (String p : pattern) {
			Resource res = Springs.searchResource(p);
			if (null != res) reses.add(res);
		}
		return reses.toArray(new Resource[reses.size()]);
	}
}
