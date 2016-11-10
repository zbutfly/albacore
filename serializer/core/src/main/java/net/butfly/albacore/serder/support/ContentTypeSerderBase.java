package net.butfly.albacore.serder.support;

import java.nio.charset.Charset;

import org.apache.http.entity.ContentType;

public abstract class ContentTypeSerderBase implements ContentTypeSerder {
	private static final long serialVersionUID = 3227054784007320028L;
	protected ContentType contentType;

	public ContentTypeSerderBase() {
		if (TextSerder.class.isAssignableFrom(getClass())) contentType = ContentTypes.TEXT_PLAIN;
		else if (BinarySerder.class.isAssignableFrom(getClass())) contentType = ContentType.APPLICATION_OCTET_STREAM;
		else contentType = ContentType.WILDCARD;
	}

	@Override
	public ContentType contentType() {
		return contentType;
	}

	@Override
	public void charset(Charset charset) {
		this.contentType = contentType.withCharset(charset);
	}
}
