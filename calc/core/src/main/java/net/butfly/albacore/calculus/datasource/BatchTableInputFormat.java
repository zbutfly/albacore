package net.butfly.albacore.calculus.datasource;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.naming.NamingException;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RegionSizeCalculator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchTableInputFormat extends org.apache.hadoop.hbase.mapreduce.TableInputFormat {
	private static final Logger logger = LoggerFactory.getLogger(BatchTableInputFormat.class);
	private static final String INITIALIZATION_ERROR = "Cannot create a record reader because of a"
			+ " previous error. Please look at the previous logs lines from" + " the task's full log for more details.";

	public BatchTableInputFormat() {
		super();
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException {
		List<InputSplit> splits;
		String offsets = getConf().get("hbase.mapreduce.batching.offsets");
		if (offsets != null && offsets.length() > 0) {
			List<byte[]> ks = new ArrayList<>();
			for (String k : offsets.split(","))
				ks.add(Bytes.toBytes(k));
			splits = baseSplits(context, ks.toArray(new byte[ks.size()][]));
		} else splits = baseSplits(context, null);
		if ((getConf().get(SHUFFLE_MAPS) != null) && "true".equals(getConf().get(SHUFFLE_MAPS).toLowerCase())) {
			Collections.shuffle(splits);
		}
		return splits;
	}

	/**
	 * Calculates the splits that will serve as input for the map tasks. The
	 * number of splits matches the number of regions in a table.
	 *
	 * @param context
	 *            The rdds job context.
	 * @return The list of input splits.
	 * @throws IOException
	 *             When creating the list of splits fails.
	 * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(
	 *      org.apache.hadoop.mapreduce.JobContext)
	 */
	@SuppressWarnings("deprecation")
	public List<InputSplit> baseSplits(JobContext context, byte[][] offsets) throws IOException {
		boolean closeOnFinish = false;

		// Just in case a subclass is relying on JobConfigurable magic.
		try {
			getTable();
		} catch (IllegalStateException e) {
			initialize(context);
			closeOnFinish = true;
		}

		// null check in case our child overrides getTable to not throw.
		try {
			if (getTable() == null) {
				// initialize() must not have been implemented in the subclass.
				throw new IOException(INITIALIZATION_ERROR);
			}
		} catch (IllegalStateException exception) {
			throw new IOException(INITIALIZATION_ERROR, exception);
		}

		try {
			RegionSizeCalculator sizeCalculator = new RegionSizeCalculator(getRegionLocator(), getAdmin());

			Pair<byte[][], byte[][]> keys = getStartEndKeys();
			if (keys == null || keys.getFirst() == null || keys.getFirst().length == 0) {
				HRegionLocation regLoc = getRegionLocator().getRegionLocation(HConstants.EMPTY_BYTE_ARRAY, false);
				if (null == regLoc) throw new IOException("Expecting at least one region.");
				List<InputSplit> splits = new ArrayList<InputSplit>(1);
				long regionSize = sizeCalculator.getRegionSize(regLoc.getRegionInfo().getRegionName());
				TableSplit split = new TableSplit(getTable().getName(), HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, regLoc
						.getHostnamePort().split(Addressing.HOSTNAME_PORT_SEPARATOR)[0], regionSize);
				splits.add(split);
				return splits;
			}
			List<InputSplit> splits = new ArrayList<InputSplit>(keys.getFirst().length);
			for (int i = 0; i < keys.getFirst().length; i++) {
				if (!includeRegionInSplit(keys.getFirst()[i], keys.getSecond()[i])) {
					continue;
				}
				HRegionLocation location = getRegionLocator().getRegionLocation(keys.getFirst()[i], false);
				// The below InetSocketAddress creation does a name resolution.
				InetSocketAddress isa = new InetSocketAddress(location.getHostname(), location.getPort());
				if (isa.isUnresolved()) logger.warn("Failed resolve " + isa);
				InetAddress regionAddress = isa.getAddress();
				String regionLocation;
				try {
					regionLocation = reverseDNS(regionAddress);
				} catch (NamingException e) {
					logger.warn("Cannot resolve the host name for " + regionAddress + " because of " + e);
					regionLocation = location.getHostname();
				}
				byte[] regionName = location.getRegionInfo().getRegionName();
				long regionSize = sizeCalculator.getRegionSize(regionName);
				// determine if the given start key fall into the region
				if (offsets == null || offsets.length == 0) splits.add(new TableSplit(getTable().getName(), keys.getFirst()[i], keys
						.getSecond()[i], regionLocation, regionSize));
				else for (byte[] startRow : offsets)
					if (startRow.length == 0 || keys.getSecond()[i].length == 0 || Bytes.compareTo(startRow, keys.getSecond()[i]) < 0) {
						byte[] splitStart = startRow.length == 0 || Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ? keys.getFirst()[i]
								: startRow;
						TableSplit split = new TableSplit(getTable().getName(), splitStart, keys.getSecond()[i], regionLocation,
								regionSize);
						splits.add(split);
						continue;
					}
			}
			return splits;
		} finally {
			if (closeOnFinish) {
				closeTable();
			}
		}
	}
}
