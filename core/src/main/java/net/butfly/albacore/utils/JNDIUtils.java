package net.butfly.albacore.utils;

import java.net.URL;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JNDIUtils extends UtilsBase {
	protected final static Logger logger = LoggerFactory.getLogger(JNDIUtils.class);

	@SuppressWarnings("unchecked")
	public static void addJNDI(String contextXML, String jndiResClassName) {
		URL url = Thread.currentThread().getContextClassLoader().getResource(contextXML);
		if (null == contextXML) return;
		Document doc;
		try {
			doc = new SAXReader().read(url);
		} catch (DocumentException e) {
			throw new RuntimeException(e);
		}
		for (Element resource : (List<Element>) doc.getRootElement().selectNodes("Resource")) {
			Object res;
			try {
				res = Class.forName(resource.attributeValue("type")).newInstance();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			if (logger.isTraceEnabled()) logger.trace("JNDI Resource added: " + XMLUtils.format(resource));
			XMLUtils.setPropsByAttr(res, resource, "name", "type");
			try {
				Class.forName(jndiResClassName).getConstructor(String.class, Object.class)
						.newInstance("java:comp/env/" + resource.attributeValue("name"), res);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
}
