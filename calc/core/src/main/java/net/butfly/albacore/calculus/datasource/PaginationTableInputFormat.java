package net.butfly.albacore.calculus.datasource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PaginationTableInputFormat extends org.apache.hadoop.hbase.mapreduce.TableInputFormat {
	private static final Logger logger = LoggerFactory.getLogger(PaginationTableInputFormat.class);
	private String nameServer;

	public PaginationTableInputFormat() {
		super();
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf = context.getConfiguration();
		HTable table = (HTable) getTable();
		if (null == table) throw new IOException("No table is provided");
		this.nameServer = conf.get("hbase.nameserver.address", null);
		logger.trace("Name server: " + this.nameServer);
		String offset = conf.get("calculus.scan.offset");
		long limit = Long.parseLong(conf.get("calculus.scan.limit"));
		// String stopKey = conf.get("calculus.stop.key");

		Pair<byte[][], byte[][]> keys = table.getRegionLocator().getStartEndKeys();
		if (keys == null || keys.getFirst() == null || keys.getFirst().length == 0)
			throw new RuntimeException("At least one region is expected.");
		List<InputSplit> splits = new ArrayList<>();
		for (int i = 0; i < keys.getFirst().length; i++) {
			String regionLocation = getTableRegionLocation(table, keys.getFirst()[i]);
		}
		// return splits;
		return super.getSplits(context);
	}

	protected String getTableRegionLocation(HTable table, byte[] rowKey) throws IOException {
		HRegionLocation loc = table.getRegionLocator().getRegionLocation(rowKey);
		return loc.toString();
	}
}
