package net.butfly.albacore.dao;

import java.util.ArrayList;
import java.util.List;

import net.butfly.albacore.entity.AbstractEntity;
import net.butfly.albacore.utils.KeyUtils;

public final class SQLBuild<E extends AbstractEntity<?>> {
	enum Verb {
		insert, delete, update, select, count
	}

	private static final String BY_CRITERIA = "ByCriteria";
	private Verb verb;
	private String namespace;
	private Class<E> entityClass;
	private List<String> suffix;

	public SQLBuild(Class<? extends EntityDAO> daoClass, Class<E> entityClass) {
		this.namespace = namespace(daoClass);
		this.entityClass = entityClass;
		this.suffix = new ArrayList<String>();
		this.suffix.add(this.entityClass.getSimpleName());
	}

	SQLBuild(String namespace, Class<E> entityClass) {
		this.namespace = namespace;
		this.entityClass = entityClass;
		this.suffix = new ArrayList<String>();
		this.suffix.add(this.entityClass.getSimpleName());
	}

	public SQLBuild<E> verb(Verb verb) {
		this.verb = verb;
		return this;
	}

	public SQLBuild<E> suffix(String... suffix) {
		for (String s : suffix)
			this.suffix.add(s);
		return this;
	}

	public String toString() {
		return namespace + verb + KeyUtils.join(suffix.toArray(new String[suffix.size()]));
	}

	public String toCriteriaString() {
		return toString() + BY_CRITERIA;
	}

	Class<E> entityClass() {
		return this.entityClass;
	}

	private static String namespace(Class<? extends EntityDAO> daoClass) {
		String namespace = daoClass.getName().replaceAll("(?i)dao", "").replaceAll("(?i)impl", ".") + ".";
		namespace = namespace.replaceAll("\\.+", ".");
		return namespace;
	}
}