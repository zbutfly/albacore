package net.butfly.albacore.dbo.criteria;

import java.util.Map;

import net.butfly.albacore.dbo.criteria.CriteriaMap.QueryType;
import net.butfly.albacore.entity.AbstractEntity;
import net.butfly.albacore.support.Bean;
import net.butfly.albacore.utils.ObjectUtils;

public class Criteria extends Bean<Criteria> {
	private static final long serialVersionUID = 4775216639071589206L;
	// public static final String ORDER_BY_PARAM_NAME = "__orderBy";
	// public static final String QUERY_TYPE_PARAM_NAME = "__queryType";
	protected CriteriaMap params;

	public Criteria() {
		this.params = new CriteriaMap();
	};

	public Criteria addOrder(String orderField) {
		this.params.addOrder(orderField);
		return this;
	}

	public Criteria addOrder(String orderField, boolean asc) {
		this.params.addOrder(orderField, asc);
		return this;
	}

	public String getOrderBy() {
		return this.params.getOrderBy();
	}

	public OrderField[] getOrderFields() {
		return this.params.getOrderFields();
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

	public CriteriaMap getParameters() {
		return this.params;
	}

	public Criteria setType(QueryType type) {
		this.params.setType(type);;
		return this;
	}
}
