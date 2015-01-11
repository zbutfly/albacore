package net.butfly.albacore.dbo.criteria;

import org.apache.ibatis.session.RowBounds;

public class OrderedRowBounds extends RowBounds {
	private OrderField[] orderFields;

	public OrderedRowBounds(RowBounds original, OrderField[] orders) {
		super(original.getOffset(), original.getLimit());
		this.orderFields = orders;
	}

	public OrderedRowBounds(int offset, int size, OrderField[] orderFields) {
		super(offset, size);
		this.orderFields = orderFields;

	}

	public OrderField[] getOrderFields() {
		return this.orderFields;
	}
}
