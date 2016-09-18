package net.butfly.albacore.serder.support;

import org.apache.http.entity.ContentType;

import com.google.common.base.Charsets;

import net.butfly.albacore.utils.Utils;

public final class ContentTypes extends Utils {
	public static ContentType APPLICATION_BSON = ContentType.create("application/bson", Charsets.UTF_8);
	public static ContentType APPLICATION_BURLAP = ContentType.create("x-application/burlap", Charsets.UTF_8);
	public static ContentType APPLICATION_HESSIAN = ContentType.create("x-application/hessian", Charsets.UTF_8);
}
