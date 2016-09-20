package net.butfly.albacore.serder;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.http.entity.ContentType;

public abstract class ContentSerderBase<PRESENT, DATA> implements ContentSerder<PRESENT, DATA> {
	private static final long serialVersionUID = -6920151785963241027L;
	protected final Map<String, ContentType> contentTypes;
	protected ContentType defaultContentType = null;

	public ContentSerderBase(ContentType... contentType) {
		super();
		contentTypes = new HashMap<>();
		if (null != contentType && contentType.length > 0) for (int i = contentType.length - 1; i >= 0; i--)
			enable(contentType[i]);
	}

	protected ContentSerderBase(ContentType base, ContentType[] supported) {
		super();
		contentTypes = new HashMap<>();
		enable(base);
		if (null != supported && supported.length > 0) for (int i = supported.length - 1; i >= 0; i--)
			enable(supported[i]);
	}

	@Override
	public ContentType contentType() {
		return null == defaultContentType ? ContentSerder.super.contentType() : defaultContentType;
	}

	@Override
	public ContentType contentType(String mimeType) {
		return contentTypes.get(mimeType);
	}

	@Override
	public Set<String> mimeTypes() {
		return contentTypes.keySet();
	}

	final protected void enable(ContentType contentType) {
		defaultContentType = contentType;
		contentTypes.put(contentType.getMimeType(), contentType);
	}
}
