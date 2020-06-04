package net.butfly.albacore.support;

import java.io.Serializable;
import java.util.Map;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;

@SuppressWarnings("unchecked")
public abstract class Desc<D extends Desc<D>> implements Serializable {
	private static final long serialVersionUID = -152619742670508854L;
	protected final Map<String, Object> attrs = Maps.of();

	public <T> T attr(String attr) {
		return (T) attrs.get(attr);
	}

	public <T> T attrm(String attr) {
		return (T) attrs.remove(attr);
	}

	public <T> T attr(String attr, Class<T> required) {
		return (T) attrs.get(attr);
	}

	public <T> T attr(String attr, T def) {
		return (T) attrs.getOrDefault(attr, def);
	}

	public <T> D attw(String attr, T value) {
		if (null != value) attrs.put(attr, value);
		else attrs.remove(attr);
		return (D) this;
	}

	public D attw(Map<String, ?> attrs) {
		if (!Colls.empty(attrs)) this.attrs.putAll(attrs);
		return (D) this;
	}

	public D attw(Desc<D> attrs) {
		return attw(attrs.attrs);
	}

	@Override
	public String toString() {
		return attrs.isEmpty() ? "" : (" (" + attrs.toString() + ")");
	}
}
