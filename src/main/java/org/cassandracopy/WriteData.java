package org.cassandracopy;

import com.datastax.driver.core.ResultSet;

public interface WriteData {

	public abstract void processResults(ResultSet rs);

	public void close();
}