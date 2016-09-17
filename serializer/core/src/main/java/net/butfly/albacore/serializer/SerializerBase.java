package net.butfly.albacore.serializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.http.entity.ContentType;

public abstract class SerializerBase<D> implements Serializer<D> {
	private final Map<String, ContentType> contentTypes;
	private final ContentType defaultContentType;

	public SerializerBase(ContentType... contentType) {
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
		return null == defaultContentType ? Serializer.super.contentType() : defaultContentType;
	}

	@Override
	public ContentType contentType(String mimeType) {
		return contentTypes.get(mimeType);
	}

	@Override
	public Set<String> supportedMimeTypes() {
		return contentTypes.keySet();
	}
}
