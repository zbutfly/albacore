package net.butfly.albacore.utils;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import net.butfly.albacore.utils.imports.meta.MetaObject;

import org.apache.xml.serialize.OutputFormat;
import org.apache.xml.serialize.XMLSerializer;
import org.dom4j.Attribute;
import org.dom4j.Element;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class XMLUtils extends UtilsBase {
	@SuppressWarnings("unchecked")
	public static void setByAttrs(Object target, Element element, String... ignores) {
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
	public static void setByNodes(Object target, Element element, String... ignores) {
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
		try {
			final org.w3c.dom.Document document = parseXmlFile(unformattedXml);

			OutputFormat format = new OutputFormat(document);
			format.setLineWidth(65);
			format.setIndenting(true);
			format.setIndent(4);
			Writer out = new StringWriter();
			XMLSerializer serializer = new XMLSerializer(out, format);
			serializer.serialize(document);

			return out.toString();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static org.w3c.dom.Document parseXmlFile(String in) {
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
