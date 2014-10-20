package net.butfly.albacore.utils.serialize;

import com.caucho.hessian.io.AbstractSerializerFactory;

public interface SerializerFactorySupport {
	void addFactory(AbstractSerializerFactory factory);
}
