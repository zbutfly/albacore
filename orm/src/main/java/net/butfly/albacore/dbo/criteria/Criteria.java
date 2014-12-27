package net.butfly.albacore.dbo.criteria;

import java.util.HashMap;
import java.util.Map;

import net.butfly.albacore.entity.AbstractEntity;
import net.butfly.albacore.support.Bean;
import net.butfly.albacore.utils.ObjectUtils;

public class Criteria extends Bean<Criteria> {
	private static final long serialVersionUID = 4775216639071589206L;
	public static final String ORDER_BY_PARAM_NAME = "__orderBy";
	public static final String QUERY_TYPE_PARAM_NAME = "__queryType";
	protected Map<String, Object> params;

	public Criteria() {
		this.params = new HashMap<String, Object>();
	};

	public Criteria addOrder(String orderField) {
		return this.addOrder(orderField, true);
	}

	public Criteria addOrder(String orderField, boolean asc) {
		String existed = (String) this.params.get(ORDER_BY_PARAM_NAME);
		StringBuilder sb = new StringBuilder(null == existed ? " ORDER BY" : existed);
		sb.append(" ").append(orderField).append(" ").append(asc ? "ASC" : "DESC");
		this.params.put(ORDER_BY_PARAM_NAME, sb.toString());
		return this;
	}

	public String getOrderBy() {
		return (String) this.params.get(ORDER_BY_PARAM_NAME);
	}

	public Criteria setParameters(Map<String, ?> params) {
		this.params.putAll(params);
		return this;
	}

	public Criteria setEntity(AbstractEntity<?> entity) {
		this.params.putAll(ObjectUtils.toMap(entity));
		return this;
	}

	public Criteria set(String key, Object value) {
		this.params.put(key, value);
		return this;
	}

	public Map<String, Object> getParameters() {
		return this.params;
	}

	public Criteria setType(QueryType type) {
		this.params.put(QUERY_TYPE_PARAM_NAME, type);
		return this;
	}

	public enum QueryType {
		COUNT, LIST
	}
}
