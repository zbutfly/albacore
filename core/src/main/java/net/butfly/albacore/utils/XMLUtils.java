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
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSOutput;
import org.w3c.dom.ls.LSSerializer;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import net.butfly.albacore.exception.NotImplementedException;

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

	private static OutputFormat format = OutputFormat.createPrettyPrint();

	public static String format(Element element) {
		StringWriter w = new StringWriter();
		XMLWriter x = new XMLWriter(w, format);
		try {
			x.write(element);
		} catch (IOException e) {}
		return w.toString();
	}

	public static String format(String unformattedXml) {
		final Document doc = parseXmlFile(unformattedXml);
		DOMImplementation impl = doc.getImplementation();
		if (impl.hasFeature("LS", "3.0") && impl.hasFeature("Core", "2.0")) {
			DOMImplementationLS ls = (DOMImplementationLS) impl.getFeature("LS", "3.0");
			LSSerializer lss = ls.createLSSerializer();
			if (lss.getDomConfig().canSetParameter("format-pretty-print", Boolean.TRUE)) {
				lss.getDomConfig().setParameter("format-pretty-print", Boolean.TRUE);
				LSOutput output = ls.createLSOutput();
				output.setEncoding("UTF-8");
				StringWriter writer = new StringWriter();
				output.setCharacterStream(writer);
				lss.write(doc, output);
				return writer.toString();
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
