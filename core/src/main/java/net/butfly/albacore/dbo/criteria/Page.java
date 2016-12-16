package net.butfly.albacore.dbo.criteria;

import net.butfly.albacore.support.Bean;

public final class Page extends Bean<Page> {
	private static final long serialVersionUID = 7953408535935745025L;
	// the first page index of the current query.
	private static final int FIRST_PAGE = 1;
	public static final int DEFAULT_PAGE_SIZE = 15;
	private static final int TOTAL_NOT_SET = -1;

	public static final Page ALL_RECORD() {
		return new Page(TOTAL_NOT_SET, Integer.MAX_VALUE, FIRST_PAGE);
	}

	public static final Page ONE_RECORD() {
		return new Page(TOTAL_NOT_SET, 1, FIRST_PAGE);
	}

	/**
	 * page information of the query, all page index should be started from 1.
	 */
	// the current page index of the current query.
	private int curr = FIRST_PAGE;
	// the next page index of the current query.
	private int next;
	// the previous page index of the current query.
	private int prev;
	// the last page index of the current query.
	private int last;

	// record counter of the page, all record index should be started from 0.
	// the first record index of the current page.
	private int start = 0;
	// the defacto count of records in the current page.
	private int size;

	/** record information of the query. */
	// current page size (max count of records can be contained in each page) of
	// the current query.
	private int capacity;
	// the total count of records of the current query.
	private int total = TOTAL_NOT_SET;

	public Page() {
		this(DEFAULT_PAGE_SIZE);
	}

	public Page(int capacity) {
		this(capacity, FIRST_PAGE);
	}

	public Page(int capacity, int curr) {
		this(TOTAL_NOT_SET, capacity, curr);
	}

	private Page(int total, int capacity, int curr) {
		if (capacity <= 0) throw new RuntimeException("0 capacity page!");
		this.capacity = capacity;
		this.total = total;
		this.setCurr(curr);
	}

	private Page calc() {
		if (!this.valid()) return this;
		if (this.total == 0) {
			this.start = 0;
			this.size = 0;
			this.curr = FIRST_PAGE;
			this.last = FIRST_PAGE;
			this.prev = FIRST_PAGE;
			this.next = FIRST_PAGE;
		} else {
			this.last = this.total % this.capacity == 0 ? this.total / this.capacity : this.total / this.capacity + 1;
			if (this.curr < FIRST_PAGE) this.curr = FIRST_PAGE;
			if (this.curr > this.last) this.curr = this.last;
			this.start = (this.curr - 1) * this.capacity;
			this.size = this.start + this.capacity > this.total ? this.total - this.start : this.capacity;
			this.prev = this.curr - 1;
			if (this.prev < FIRST_PAGE) this.prev = FIRST_PAGE;
			this.next = this.curr + 1;
			if (this.next > this.last) this.next = this.last;
		}
		return this;
	}

	public Page setCurr(int curr) {
		this.curr = curr;
		return this.calc();
	}

	public Page setTotal(int total) {
		this.total = total;
		return this.calc();
	}

	public Page setCapacity(int capacity) {
		this.capacity = capacity;
		return this.calc();
	}

	public int getCurr() {
		return this.curr;
	}

	public int getTotal() {
		return this.total;
	}

	public int getCapacity() {
		return capacity;
	}

	public int getNext() {
		return next;
	}

	public int getPrev() {
		return prev;
	}

	public int getFirst() {
		return FIRST_PAGE;
	}

	public int getLast() {
		return last;
	}

	@Deprecated
	public int getStart() {
		return start;
	}

	@Deprecated
	public int getSize() {
		return size;
	}

	public boolean valid() {
		return total >= 0 && capacity > 0;
	}

	public boolean empty() {
		return size > 0;
	}

	public int getOffset() {
		return start;
	}

	public int getLimit() {
		return size;
	}
}
