package net.butfly.albacore.serder;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.http.entity.ContentType;

public abstract class ContentSerderBase<PRESENT, DATA> extends SerderBase<PRESENT, DATA> implements ContentSerder<PRESENT, DATA> {
	private static final long serialVersionUID = -6920151785963241027L;
	protected final Map<String, ContentType> contentTypes;
	protected ContentType defaultContentType;

	public ContentSerderBase(ContentType... contentType) {
		super();
		contentTypes = new HashMap<>();
		if (contentType != null && contentType.length == 0) {
			defaultContentType = contentType[0];
			for (ContentType ct : contentType)
				contentTypes.put(ct.getMimeType(), ct);
		} else defaultContentType = null;
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
	public Set<String> supportedMimeTypes() {
		return contentTypes.keySet();
	}

	final protected void enable(ContentType contentType) {
		if (defaultContentType == null) defaultContentType = contentType;
		if (!contentTypes.containsKey(contentType.getMimeType())) contentTypes.put(contentType.getMimeType(), contentType);
	}
}
