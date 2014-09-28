package org.cassandracopy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class IBSACassandraWriter implements WriteData {
	private static final Logger logger = LoggerFactory
			.getLogger(IBSACassandraWriter.class);

	private CassandraConnection connection;
	private Session session;

	public IBSACassandraWriter() {
		connection = new CassandraConnection();
		connection
				.setCassandraConnectionPropertiesFile("CassandraWriter.properties");
		connection.init();
		session = connection.connect();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * mapreduce.WriteData#processResults(com.datastax.driver.core.ResultSet)
	 */
	public void processResults(ResultSet rs) {
		StringBuffer buffer = new StringBuffer(10000);
		buffer.append("BEGIN BATCH\n");
		int rowCount = 0;

		while (rs.iterator().hasNext()) {
			Row row = rs.iterator().next();
			long id = row.getLong("id");
			long lid = row.getLong("lid");

			String text = row.getString("ibsa");
			String insert = "INSERT INTO ccrc_ibsa_hatinder (id, lid, ibsa) VALUES ( "
					+ id + ", " + lid + ", '" + text + "')\n";
			buffer.append(insert);
			rowCount++;
		}
		buffer.append("APPLY BATCH;");
		logger.debug("Batch Row Count : " + rowCount);
		session.execute(buffer.toString());
	}

	public void close() {
	}
}
