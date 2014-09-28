package org.cassandracopy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class FileWriter implements WriteData {
	private static final Logger logger = LoggerFactory
			.getLogger(FileWriter.class);

	private FileOutputStream os;

	public FileWriter() throws java.io.FileNotFoundException {
		File file = new File("Output-file");
		os = new FileOutputStream(file);
		logger.info(file.getParent() + ":" + file.getPath());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * mapreduce.WriteData#processResults(com.datastax.driver.core.ResultSet)
	 */
	public void processResults(ResultSet rs) {
		int rowCount = 0;

		while (rs.iterator().hasNext()) {
			Row row = rs.iterator().next();
			long id = row.getLong("id");
			long lid = row.getLong("lid");

			String text = row.getString("ibsa");
			String line = text + "\n";
			try {
				synchronized (os) {
					os.write(line.getBytes());
					os.flush();
				}
			} catch (IOException e) {
				logger.error(e.getMessage());
				throw new RuntimeException(e.getMessage());
			}
			rowCount++;
		}
		logger.debug("Batch Row Count : " + rowCount);
	}

	public void close() {
		try {
			os.close();
		} catch (IOException e) {
			logger.error(e.getMessage());
			throw new RuntimeException(e.getMessage());
		}
	}
}
