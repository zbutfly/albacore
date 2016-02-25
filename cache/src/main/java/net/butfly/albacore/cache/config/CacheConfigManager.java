package net.butfly.albacore.cache.config;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import net.butfly.albacore.cache.utils.methodintrude.BaseMethodIntrudeBase;
import net.butfly.albacore.cache.utils.strategy.ICacheStrategy;
import net.butfly.albacore.cache.utils.strategy.keygenerate.IKeyGenerator;
import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.utils.Reflections;

@SuppressWarnings({ "unchecked" })
public class CacheConfigManager {
	private final static Logger logger = LoggerFactory.getLogger(CacheConfigManager.class);
	public static final String CACHE_TYPE_BASE = "base";
	public static final String OPTYPE_USER = "use";
	public static final String OPTYPE_DESTORY = "destory";
	private static Map<String, String> configCacheTypeMap = new HashMap<String, String>();
	private static Map<String, String> configStrategyIdMap = new HashMap<String, String>();
	private static Map<String, ICacheStrategy> strategysMap = new HashMap<String, ICacheStrategy>();
	private static Set<Method> methodsMap = new HashSet<Method>();
	private static Map<Method, String> configsMethodMap = new HashMap<Method, String>();

	public void load(String classPath) throws SystemException {
		try {
			SAXReader reader = new SAXReader();
			Resource rs = new ClassPathResource(classPath);
			InputStream in;
			in = rs.getInputStream();
			Document document = reader.read(in);
			// readXmlFile
			// strategys Part
			// keygenerators Part
			List<Element> list = document.selectNodes("/elements/strategys/keygenerators/keygenerator");
			Map<String, String> keygenerators = new HashMap<String, String>();
			for (Element el : list) {
				keygenerators.put(el.valueOf("@id"), el.valueOf("@class"));
			}
			// memcacheclient Part
			// 缓存客户端定义及配置 放在spring 配置文件 beans-cache.xml
			// 中注入到MemCacheClinetImpl.java
			// strategy Part
			List<Element> strategyList = document.selectNodes("/elements/strategys/strategy");
			for (Element el : strategyList) {
				String id = el.valueOf("@id");
				IKeyGenerator keyG = Reflections.construct(keygenerators.get(el.selectSingleNode("keygenerator").valueOf("@ref")));
				int expiration = Integer.parseInt(el.selectSingleNode("expiration").valueOf("@value"));
				ICacheStrategy strategy = new ICacheStrategy(keyG, expiration);
				strategysMap.put(id, strategy);
			}
			// element Part
			List<Element> elementList = document.selectNodes("/elements/element");
			for (Element el : elementList) {
				String id = el.valueOf("@id");
				String type = el.valueOf("@type");
				configCacheTypeMap.put(id, type);
				configStrategyIdMap.put(id, el.valueOf("@strategy"));
				String methodIntrude_str = el.valueOf("@methodIntrude");
				if (null != methodIntrude_str && !"".equals(methodIntrude_str)) {
					if (CACHE_TYPE_BASE.equals(type)) {
						BaseMethodIntrudeBase methodIntrude = Reflections.construct(el.valueOf("@methodIntrude"));
						Set<Method> methods = methodIntrude.getUseMethods();
						opAndconfigsAndmethodsMapPut(methods, id);
					}
				}
			}
			logger.debug(" read cache.xml successed !!!!!!");
			logger.debug(" configCacheTypeMap : \n" + configCacheTypeMap);
			logger.debug(" configStrategyIdMap : \n" + configStrategyIdMap);
			logger.debug(" strategysMap : \n" + strategysMap);
			logger.debug(" methodsMap : \n" + methodsMap);
			logger.debug(" configsMethodMap : \n" + configsMethodMap);
		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
			throw new SystemException("SYS_121", e);
		} catch (DocumentException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
			throw new SystemException("SYS_121", e);
		}
	}

	private static void opAndconfigsAndmethodsMapPut(Set<Method> methods, String configId) {
		methodsMap.addAll(methods);
		for (Method method : methods) {
			configsMethodMap.put(method, configId);
		}
	}

	public static String getCacheType(String configId) {
		return configCacheTypeMap.get(configId);
	}

	public static String getStrategyId(String configId) {
		return configStrategyIdMap.get(configId);
	}

	public static Map<String, ICacheStrategy> getStrategys() {
		return strategysMap;
	}

	public static boolean isAutoProxy(Method method) {
		return methodsMap.contains(method);
	}

	public static String getConfigsByMethod(Method method) {
		return configsMethodMap.get(method);
	}
}
