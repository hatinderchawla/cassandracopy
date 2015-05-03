package org.cassandracopy;

import com.datastax.driver.core.ResultSet;

public interface WriteData {

	public abstract int processResults(ResultSet rs);

	public void close();
}