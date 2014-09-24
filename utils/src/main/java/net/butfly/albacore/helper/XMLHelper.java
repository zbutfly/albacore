package net.butfly.albacore.helper;

import java.io.InputStream;
import java.io.Reader;
import java.util.List;

import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.Element;

public interface XMLHelper extends Helper {
	Document getDocument(Reader pReader);

	Document getDocument(InputStream pIS);

	Document getDocument(String pFileName);

	Element getRootElement(Document document);

	List<Element> getSubElements(Element element);

	List<Element> getSubElements(Element element, String name);

	Element getSubElement(Element element, String name);

	String getSubElementValue(Element element, String name);

	List<?> getSubElementAsAttr(Element element, String name, String attrName, String attrValue);

	List<?> getAttributes(Element element);

	Attribute getAttribute(Element element, String name);

	String getAttributeValue(Element element, String name);

	String getText(Element element);
}
