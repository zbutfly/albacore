package net.butfly.albacore.helper.xml;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.helper.HelperBase;
import net.butfly.albacore.helper.XMLHelper;

import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

@SuppressWarnings("unchecked")
public class XMLHelperImpl extends HelperBase implements XMLHelper {
	private static final long serialVersionUID = -4707024323691670456L;

	@Override
	public Document getDocument(Reader pReader) {
		if (pReader == null) return null;
		SAXReader reader = new SAXReader();
		Document document;
		try {
			document = reader.read(pReader);
		} catch (DocumentException e) {
			logger.error(e.getMessage(), e);
			throw new SystemException("SYS003", "加载XML文档失败!", e);
		}
		return document;
	}

	@Override
	public Document getDocument(InputStream is) {
		if (is == null) return null;
		SAXReader reader = new SAXReader();
		Document document;
		try {
			document = reader.read(is);
		} catch (DocumentException e) {
			logger.error(e.getMessage(), e);
			throw new SystemException("SYS003", "加载XML文档失败!", e);
		}
		return document;
	}

	@Override
	public Document getDocument(String filename) {
		BufferedReader in = null;
		try {
			in = new BufferedReader(new FileReader(filename));
		} catch (FileNotFoundException e1) {
			logger.error(e1.getMessage(), e1);
			throw new SystemException("SYS004", "读取XML文档失败!", e1);
		}
		SAXReader reader = new SAXReader();
		Document document;
		try {
			document = reader.read(in);
		} catch (DocumentException e) {
			logger.error(e.getMessage(), e);
			throw new SystemException("SYS003", "加载XML文档失败!", e);
		}
		return document;
	}

	@Override
	public Element getRootElement(Document document) {
		if (document == null) return null;
		return document.getRootElement();
	}

	@Override
	public List<Element> getSubElements(Element element) {
		if (element == null) return null;
		List<Element> result = new ArrayList<Element>();
		for (Iterator<Element> iterator = element.elementIterator(); iterator.hasNext();) {
			result.add((Element) iterator.next());
		}
		return result;
	}

	@Override
	public List<Element> getSubElements(Element element, String name) {
		if (element == null) return null;
		List<Element> result = element.selectNodes("//" + name);
		if (result == null) return Collections.emptyList();
		return result;
	}

	@Override
	public Element getSubElement(Element element, String name) {
		if (element == null) return null;
		Element elm = (Element) element.selectSingleNode("//" + name);
		return elm;
	}

	@Override
	public String getSubElementValue(Element element, String name) {
		Element elm = getSubElement(element, name);
		if (elm == null) return null;
		return elm.getTextTrim();

	}

	@Override
	public List<?> getSubElementAsAttr(Element element, String name, String attrName, String attrValue) {
		if (element == null) return null;
		StringBuffer buffer = new StringBuffer("//");
		buffer.append(name);
		buffer.append("[@");
		buffer.append(attrName);
		buffer.append("='");
		buffer.append(attrValue);
		buffer.append("']");
		List<?> result = element.selectNodes(buffer.toString());
		return result;

	}

	@Override
	public List<?> getAttributes(Element element) {
		if (element == null) return null;
		List<?> result = element.attributes();
		if (result == null) return Collections.EMPTY_LIST;
		return result;
	}

	@Override
	public Attribute getAttribute(Element element, String name) {
		if (element == null) return null;
		Attribute attr = (Attribute) element.selectSingleNode("./@" + name);
		return attr;
	}

	@Override
	public String getAttributeValue(Element element, String name) {
		if (element == null) return null;
		Attribute attr = getAttribute(element, name);
		if (attr == null) return null;
		return attr.getValue();
	}

	@Override
	public String getText(Element element) {
		if (element == null) return null;
		return element.getTextTrim();
	}

}
