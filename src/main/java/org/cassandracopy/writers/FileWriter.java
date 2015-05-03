package org.cassandracopy.writers;

import org.cassandracopy.WriteData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

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
	public int processResults(ResultSet rs) {
		int rowCount = 0;

		while (rs.iterator().hasNext()) {
			Row row = rs.iterator().next();

			ColumnDefinitions columns = row.getColumnDefinitions();
			while (columns.iterator().hasNext()) {
			Definition column = columns.iterator().next();
			ByteBuffer bytes = row.getBytesUnsafe(column.getName());			
			try {
				synchronized (os) {
					os.write(bytes.array());
				}
			} catch (IOException e) {
				logger.error(e.getMessage());
				throw new RuntimeException(e.getMessage());
			}
			}
			rowCount++;
		}
		logger.debug("Batch Row Count : " + rowCount);
		return rowCount;
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
