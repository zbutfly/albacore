package net.butfly.albacore.utils.more;

import java.net.URL;
import java.util.Iterator;
import java.util.List;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.Name;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.StringRefAddr;
import javax.naming.spi.ObjectFactory;

import net.butfly.albacore.utils.Objects;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.Utils;

import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import net.butfly.albacore.utils.logger.Logger;

public final class JNDIUtils extends Utils {
	protected static final Logger logger = Logger.getLogger(JNDIUtils.class);

	public static void attachContext(String contextFileLocation) {
		try {
			JNDIUtils.bindContext("java:comp/env/", contextFileLocation);
		} catch (NamingException e) {
			throw new RuntimeException("Failure in JNDI process", e);
		}
	}

	@SuppressWarnings("unchecked")
	public static void bindContext(String parent, String contextXML) throws NamingException {
		URL url = Thread.currentThread().getContextClassLoader().getResource(contextXML);
		if (null == contextXML) return;
		Document doc;
		try {
			doc = new SAXReader().read(url);
		} catch (DocumentException e) {
			logger.error("Context XML parsing failure", e);
			return;
		}

		Context env = JNDIUtils.confirmName(new InitialContext(), parent);

		for (Element res : (List<Element>) doc.getRootElement().selectNodes("Resource")) {
			String name = res.attributeValue("name");
			if (name == null) logger.error("Resource name not defined");
			else try {
				JNDIUtils.confirmName(env, name);
				env.bind(name, JNDIUtils.parseResource(env, res));
				logger.trace("JNDI [" + name + "] in [" + env.getNameInNamespace() + "] binded.");
			} catch (Exception e) {
				logger.error("Resource parsing failure", e);
			}
		}
	}

	private static Object parseResource(Context ctx, Element resource) throws Exception {
		if (logger.isTraceEnabled()) logger.trace("JNDI Resource parsing: " + XMLUtils.format(resource));

		String fact = resource.attributeValue("factory");
		String type = resource.attributeValue("type");
		if (Objects.isEmpty(fact)) {
			Object ds = Reflections.construct(type);
			Objects.noneNull(ds);
			XMLUtils.setPropsByAttr(ds, resource, "name", "type");
			return ds;
		} else {
			ObjectFactory factory = Reflections.construct(fact);
			Object ds = factory.getObjectInstance(parseReference(resource),
					ctx.getNameParser(ctx.getNameInNamespace()).parse(ctx.getNameInNamespace()), ctx, ctx.getEnvironment());
			return ds;
		}
	}

	@SuppressWarnings("unchecked")
	private static Object parseReference(Element resource) {
		Reference ref = new Reference(resource.attributeValue("type"));
		Iterator<Attribute> it = resource.attributeIterator();
		while (it.hasNext()) {
			Attribute attr = it.next();
			ref.add(new StringRefAddr(attr.getName(), attr.getValue()));
		}
		return ref;
	}

	private static Context confirmName(Context ctx, String name) throws NamingException {
		Name nm = ctx.getNameParser(ctx.getNameInNamespace()).parse(name);
		for (int i = 0; i < nm.size() - 1; i++) {
			try {
				ctx = (Context) ctx.lookup(nm.get(i));
			} catch (NameNotFoundException ex) {
				ctx = ctx.createSubcontext(nm.get(i));
				logger.trace("JNDI [" + ctx.getNameInNamespace() + "] created.");
			}
		}
		return ctx;
	}
}
