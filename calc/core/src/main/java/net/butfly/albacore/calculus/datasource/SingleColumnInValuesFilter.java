package net.butfly.albacore.calculus.datasource;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleColumnInValuesFilter extends FilterBase {
	protected static final Logger logger = LoggerFactory.getLogger(SingleColumnInValuesFilter.class);

	protected byte[] columnFamily;
	protected byte[] columnQualifier;
	protected ByteArrayComparable[] comparators;
	protected boolean foundColumn = false;
	protected boolean matchedColumn = false;
	protected boolean filterIfMissing = true;
	protected boolean latestVersionOnly = true;

	/**
	 * Constructor for binary compare of the value of a single column. If the
	 * column is found and the condition passes, all columns of the row will be
	 * emitted. If the condition fails, the row will not be emitted.
	 * <p>
	 * Use the filterIfColumnMissing flag to set whether the rest of the columns
	 * in a row will be emitted if the specified column to check is not found in
	 * the row.
	 *
	 * @param family
	 *            name of column family
	 * @param qualifier
	 *            name of column qualifier
	 * @param compareOp
	 *            operator
	 * @param value
	 *            value to compare column values against
	 */
	public SingleColumnInValuesFilter(final byte[] family, final byte[] qualifier, final byte[]... values) {
		this(family, qualifier, tocomp(values));
	}

	private static ByteArrayComparable[] tocomp(final byte[][] values) {
		ByteArrayComparable[] cs = new ByteArrayComparable[values.length];
		for (int i = 0; i < values.length; i++)
			cs[i] = new BinaryComparator(values[i]);
		return cs;
	}

	/**
	 * Constructor for binary compare of the value of a single column. If the
	 * column is found and the condition passes, all columns of the row will be
	 * emitted. If the condition fails, the row will not be emitted.
	 * <p>
	 * Use the filterIfColumnMissing flag to set whether the rest of the columns
	 * in a row will be emitted if the specified column to check is not found in
	 * the row.
	 *
	 * @param family
	 *            name of column family
	 * @param qualifier
	 *            name of column qualifier
	 * @param compareOp
	 *            operator
	 * @param comparators
	 *            Comparator to use.
	 */
	public SingleColumnInValuesFilter(final byte[] family, final byte[] qualifier, final ByteArrayComparable... comparator) {
		this.columnFamily = family;
		this.columnQualifier = qualifier;
		this.comparators = comparator;
	}

	/**
	 * Constructor for protobuf deserialization only.
	 * 
	 * @param family
	 * @param qualifier
	 * @param compareOp
	 * @param comparators
	 * @param filterIfMissing
	 * @param latestVersionOnly
	 */
	protected SingleColumnInValuesFilter(final byte[] family, final byte[] qualifier, final CompareOp compareOp,
			ByteArrayComparable comparator, final boolean filterIfMissing, final boolean latestVersionOnly) {
		this(family, qualifier, comparator);
		this.filterIfMissing = filterIfMissing;
		this.latestVersionOnly = latestVersionOnly;
	}

	/**
	 * @return the comparators
	 */
	public ByteArrayComparable[] getComparator() {
		return comparators;
	}

	/**
	 * @return the family
	 */
	public byte[] getFamily() {
		return columnFamily;
	}

	/**
	 * @return the qualifier
	 */
	public byte[] getQualifier() {
		return columnQualifier;
	}

	@Override
	public ReturnCode filterKeyValue(Cell c) {
		if (this.matchedColumn) {
			// We already found and matched the single column, all keys now pass
			return ReturnCode.INCLUDE;
		} else if (this.latestVersionOnly && this.foundColumn) {
			// We found but did not match the single column, offset to next row
			return ReturnCode.NEXT_ROW;
		}
		if (!CellUtil.matchingColumn(c, this.columnFamily, this.columnQualifier)) { return ReturnCode.INCLUDE; }
		foundColumn = true;
		if (filterColumnValue(c.getValueArray(), c.getValueOffset(), c.getValueLength())) { return this.latestVersionOnly
				? ReturnCode.NEXT_ROW : ReturnCode.INCLUDE; }
		this.matchedColumn = true;
		return ReturnCode.INCLUDE;
	}

	// Override here explicitly as the method in super class FilterBase might do
	// a KeyValue recreate.
	// See HBASE-12068
	@Override
	public Cell transformCell(Cell v) {
		return v;
	}

	private boolean filterColumnValue(final byte[] data, final int offset, final int length) {
		for (ByteArrayComparable bc : this.comparators) {
			int compareResult = bc.compareTo(data, offset, length);
			if (compareResult != 0) return false;
		}
		return true;
	}

	public boolean filterRow() {
		// If column was found, return false if it was matched, true if it was
		// not
		// If column not found, return true if we filter if missing, false if
		// not
		return this.foundColumn ? !this.matchedColumn : this.filterIfMissing;
	}

	public boolean hasFilterRow() {
		return true;
	}

	public void reset() {
		foundColumn = false;
		matchedColumn = false;
	}

	/**
	 * Get whether entire row should be filtered if column is not found.
	 * 
	 * @return true if row should be skipped if column not found, false if row
	 *         should be let through anyways
	 */
	public boolean getFilterIfMissing() {
		return filterIfMissing;
	}

	/**
	 * Set whether entire row should be filtered if column is not found.
	 * <p>
	 * If true, the entire row will be skipped if the column is not found.
	 * <p>
	 * If false, the row will pass if the column is not found. This is default.
	 * 
	 * @param filterIfMissing
	 *            flag
	 */
	public void setFilterIfMissing(boolean filterIfMissing) {
		this.filterIfMissing = filterIfMissing;
	}

	/**
	 * Get whether only the latest version of the column value should be
	 * compared. If true, the row will be returned if only the latest version of
	 * the column value matches. If false, the row will be returned if any
	 * version of the column value matches. The default is true.
	 * 
	 * @return return value
	 */
	public boolean getLatestVersionOnly() {
		return latestVersionOnly;
	}

	/**
	 * Set whether only the latest version of the column value should be
	 * compared. If true, the row will be returned if only the latest version of
	 * the column value matches. If false, the row will be returned if any
	 * version of the column value matches. The default is true.
	 * 
	 * @param latestVersionOnly
	 *            flag
	 */
	public void setLatestVersionOnly(boolean latestVersionOnly) {
		this.latestVersionOnly = latestVersionOnly;
	}

	/**
	 * The only CF this filter needs is given column family. So, it's the only
	 * essential column in whole scan. If filterIfMissing == false, all families
	 * are essential, because of possibility of skipping the rows without any
	 * data in filtered CF.
	 */
	public boolean isFamilyEssential(byte[] name) {
		return !this.filterIfMissing || Bytes.equals(name, this.columnFamily);
	}

	@Override
	public String toString() {
		return String.format("%s (%s, %s, %d)", this.getClass().getSimpleName(), Bytes.toStringBinary(this.columnFamily), Bytes
				.toStringBinary(this.columnQualifier), this.comparators.length);
	}
}
