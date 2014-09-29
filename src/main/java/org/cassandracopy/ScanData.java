package org.cassandracopy;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;

;

public class ScanData {
	static byte[] testBytes = new byte[1];
	private Session session;
	private ExecutorService executor;
	private WriteData writer;

	private static final Logger logger = LoggerFactory
			.getLogger(ScanData.class);

	private String query = "Select * from " + columnFamily + "  where token("
			+ primaryKey + ") >= ? AND token(" + primaryKey + ") <= ? LIMIT 10000";

	private PreparedStatement preparedStatement;

	@Argument(alias = "t", description = "Number of Threads", required = false)
	private static Integer threadPoolSize = 10;

	@Argument(alias = "s", description = "Number of Segments", required = true)
	private static Integer numSegments = 100;

	@Argument(alias = "b", description = "Number of batches per segment", required = false)
	private static Integer numBatchesPerSegment = 1000;

	@Argument(alias = "ss", description = "Starting segment", required = false)
	private static Integer startSegment = 1;

	@Argument(alias = "es", description = "Starting segment", required = false)
	private static Integer endSegment = 2;

	@Argument(alias = "k", description = "Keyspace name", required = false)
	private static String keyspace = "ibsa";

	@Argument(alias = "c", description = "Column Family name", required = false)
	private static String columnFamily = "ccrc_ibsa";

	@Argument(alias = "w", description = "WriterClass", required = false)
	private static String writerClassName = "mapreduce.BlankWriter";

	@Argument(alias = "p", description = "Primary Key", required = true)
	private static String primaryKey;

	public static void main(String[] args) {
		final List<String> parse;

		try {
			parse = Args.parse(ScanData.class, args);
			ScanData rd = new ScanData();
			long startTime = System.currentTimeMillis();
			rd.processAll();
			long endTime = System.currentTimeMillis();
			logger.info("Done. Total time taken = " + (endTime - startTime));
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
			Args.usage(ScanData.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(1);
		return;
	}

	public ScanData() throws Exception {
		if (startSegment < 1) {
			startSegment = 1;
		}

		if ((endSegment <= 0) || (endSegment > numSegments)) {
			endSegment = numSegments;
		}

		Class<?> writerClass = Class.forName(writerClassName);
		writer = (WriteData) writerClass.newInstance();

		session = CassandraConnection.getSession(keyspace);

		preparedStatement = session.prepare(query);

	}

	protected void processAll() {

		long startTime = System.currentTimeMillis();

		long perSegmentTokens = (Long.MAX_VALUE / numSegments)
				- (Long.MIN_VALUE / numSegments);

		for (int i = (startSegment - 1); i < endSegment; i++) {
			long segmentStartToken = Long.MIN_VALUE + i * perSegmentTokens;
			long segmentEndToken = segmentStartToken + perSegmentTokens;
			processSegment(i, segmentStartToken, segmentEndToken);
			logger.info("Segment complete " + (i + 1));
		}

		long endTime = System.currentTimeMillis();

		writer.close();

		logger.info((endSegment - (startSegment - 1))
				+ " segments completed in " + (endTime - startTime) + " ms");
	}

	public void processSegment(int segmentNumber, long segmentStartToken,
			long segmentEndToken) {
		long startTime = System.currentTimeMillis();

		executor = Executors.newFixedThreadPool(threadPoolSize);

		long perBatchTokens = segmentEndToken / numBatchesPerSegment
				- segmentStartToken / numBatchesPerSegment;

		for (int i = 0; i < numBatchesPerSegment; i++) {
			long startToken = segmentStartToken + i * perBatchTokens;
			long endToken = startToken + perBatchTokens;
			Runnable runnable = this.new GetRows(i + 1, startToken, endToken);
			executor.execute(runnable);
		}

		executor.shutdown();

		// Wait until all threads are finish
		try {
			executor.awaitTermination(1000000000L, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		long endTime = System.currentTimeMillis();

		logger.info("Segment " + segmentNumber + " completed in "
				+ (endTime - startTime) + " ms");

	}

	public class GetRows implements Runnable {

		int batchNumber;
		long startToken;
		long endToken;

		public GetRows(int i, long startToken, long endToken) {
			batchNumber = i;
			this.startToken = startToken;
			this.endToken = endToken;
		}

		public void run() {

			try {
				processBatch();

			} catch (Exception e) {
				logger.error(e.getMessage());
				e.printStackTrace();
				logger.warn("Batch failure " + batchNumber);
				logger.warn("Retrying batch post failure " + batchNumber);
				try {
					processBatch();
				} catch (Exception ex) {
					logger.error("Batch Retry failed " + batchNumber);
				}
			}
		}

		private void processBatch() {
			long batchStartTime = System.currentTimeMillis();
			
			BoundStatement stmt = new BoundStatement(preparedStatement);

			stmt.bind(startToken, endToken);
			
			ResultSet rs = session.execute(stmt);

			writer.processResults(rs);

			long batchEndTime = System.currentTimeMillis();

			logger.debug("Thread " + Thread.currentThread().getName()
					+ ". Batch " + this.batchNumber + " completed in "
					+ (batchEndTime - batchStartTime) + " ms");
		}
	}
}
