package net.butfly.albacore.helper.xml;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * @author ho274543
 */
/**
 * @author ho274543
 */
public abstract class AbstractSAXParser extends DefaultHandler {

	private static SAXParserFactory FACTORY = SAXParserFactory.newInstance();

	private StringBuilder value = null;

	public static interface ParserCallback {

		void fetch(Object object);
	}

	protected ParserCallback callback;
	private String filename;

	// private Stack<String> tags = new Stack<String>();

	public AbstractSAXParser(String filename, ParserCallback callback) {
		this.filename = filename;
		this.callback = callback;
	}

	public void parse() throws ParserConfigurationException, SAXException, IOException {
		FACTORY.newSAXParser().parse(new InputSource(filename), this);
	}

	@Override
	public final void startElement(String uri, String localName, String qName, Attributes attrs) {
		this.value = new StringBuilder();
		this.start(qName);
	}

	@Override
	public final void endElement(String uri, String localName, String qName) throws SAXException {
		this.callback.fetch(this.end(qName, this.value.toString()));
		// TODO: need optimizing...
		this.value = new StringBuilder();
	}

	@Override
	public final void characters(char ch[], int start, int length) throws SAXException {
		this.value.append(new String(ch, start, length));
	}

	/**
	 * finilize the node object, return node value free and delete them for
	 * preparing of the next node with same class.
	 * 
	 * @param tag
	 * @param string
	 * @return
	 */
	abstract protected Object end(String tag, String value);

	/**
	 * initialize the node object, create it to recieve its properties.
	 * 
	 * @param tag
	 */
	abstract protected void start(String tag);
}
