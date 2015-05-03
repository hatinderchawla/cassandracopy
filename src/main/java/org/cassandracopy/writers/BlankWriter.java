package org.cassandracopy.writers;

import org.cassandracopy.WriteData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class BlankWriter implements WriteData {
	private static final Logger logger = LoggerFactory
			.getLogger(BlankWriter.class);

	public int processResults(ResultSet rs) {
		// TODO Auto-generated method stub
		int rowCount = 0;

		while (rs.iterator().hasNext()) {
			Row row = rs.iterator().next();
			rowCount++;
		}
		logger.debug("Batch Row count " + rowCount);
		return rowCount;
	}

	public void close() {
	}

}
