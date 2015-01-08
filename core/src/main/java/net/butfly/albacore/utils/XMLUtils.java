package net.butfly.albacore.utils;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import net.butfly.albacore.utils.imports.meta.MetaObject;

import org.dom4j.Attribute;
import org.dom4j.Element;
import org.w3c.dom.DOMConfiguration;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSOutput;
import org.w3c.dom.ls.LSSerializer;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class XMLUtils extends UtilsBase {
	@SuppressWarnings("unchecked")
	public static void setPropsByAttr(Object target, Element element, String... ignores) {
		MetaObject meta = ObjectUtils.createMeta(target);
		Iterator<Attribute> it = element.attributeIterator();
		while (it.hasNext()) {
			Attribute attr = it.next();
			String name = attr.getName();
			if (ignores != null) for (String ig : ignores)
				if (name.equals(ig)) continue;
			if (meta.hasSetter(name)) meta.setValue(name, ObjectUtils.castValue(attr.getValue(), meta.getSetterType(name)));
		}
	}

	@SuppressWarnings("unchecked")
	public static void setPropsByNode(Object target, Element element, String... ignores) {
		MetaObject meta = ObjectUtils.createMeta(target);
		for (Element node : (List<Element>) element.selectNodes("*")) {
			String name = node.getName();
			if (ignores != null) for (String ig : ignores)
				if (name.equals(ig)) continue;
			if (meta.hasSetter(name)) {
				if (!meta.getSetterType(name).isArray()) {
					String value = node.getTextTrim();
					meta.setValue(name, ObjectUtils.castValue(value, meta.getSetterType(name)));
				} else {
					throw new NotImplementedException();
				}
			}
		}
	}

	public static String format(String unformattedXml) {
		// Pretty-prints a DOM document to XML using DOM Load and Save's LSSerializer.
		// Note that the "format-pretty-print" DOM configuration parameter can only be set in JDK 1.6+.
		final Document document = parseXmlFile(unformattedXml);
		DOMImplementation domImplementation = document.getImplementation();
		if (domImplementation.hasFeature("LS", "3.0") && domImplementation.hasFeature("Core", "2.0")) {
			DOMImplementationLS domImplementationLS = (DOMImplementationLS) domImplementation.getFeature("LS", "3.0");
			LSSerializer lsSerializer = domImplementationLS.createLSSerializer();
			DOMConfiguration domConfiguration = lsSerializer.getDomConfig();
			if (domConfiguration.canSetParameter("format-pretty-print", Boolean.TRUE)) {
				lsSerializer.getDomConfig().setParameter("format-pretty-print", Boolean.TRUE);
				LSOutput lsOutput = domImplementationLS.createLSOutput();
				lsOutput.setEncoding("UTF-8");
				StringWriter stringWriter = new StringWriter();
				lsOutput.setCharacterStream(stringWriter);
				lsSerializer.write(document, lsOutput);
				return stringWriter.toString();
			} else {
				throw new RuntimeException("DOMConfiguration 'format-pretty-print' parameter isn't settable.");
			}
		} else {
			throw new RuntimeException("DOM 3.0 LS and/or DOM 2.0 Core not supported.");
		}
	}

	private static Document parseXmlFile(String in) {
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			InputSource is = new InputSource(new StringReader(in));
			return db.parse(is);
		} catch (ParserConfigurationException e) {
			throw new RuntimeException(e);
		} catch (SAXException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
