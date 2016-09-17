package net.butfly.albacore.serializer;

import java.io.Serializable;
import java.util.Set;

import org.apache.http.entity.ContentType;

import com.google.common.base.Charsets;

public interface Serializer<D> {
	// XXX: need Serializable? or Type?
	D serialize(Serializable src);

	Serializable deserialize(D dst, Class<? extends Serializable> srcClass);

	default ContentType contentType() {
		return ContentType.WILDCARD.withCharset(Charsets.UTF_8);
	}

	ContentType contentType(String mimeType);

	Set<String> supportedMimeTypes();

	Serializable[] deserialize(D dst, Class<? extends Serializable>[] types);
}
