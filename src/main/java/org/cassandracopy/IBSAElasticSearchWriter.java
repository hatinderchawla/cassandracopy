package org.cassandracopy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class IBSAElasticSearchWriter implements WriteData {
	private static final Logger logger = LoggerFactory
			.getLogger(IBSAElasticSearchWriter.class);

	private Client elasticClient;

	public IBSAElasticSearchWriter() {
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", "UCS_STAGE_ES_CLUSTER").build();

		elasticClient = new TransportClient(settings)
				.addTransportAddress(
						new InetSocketTransportAddress(
								"ccrc-elastic-01-os.cisco.com", 9300))
				.addTransportAddress(
						new InetSocketTransportAddress(
								"ccrc-elastic-02-os.cisco.com", 9300))
				.addTransportAddress(
						new InetSocketTransportAddress(
								"ccrc-elastic-03-os.cisco.com", 9300))
				.addTransportAddress(
						new InetSocketTransportAddress(
								"ccrc-elastic-04-os.cisco.com", 9300));

	}

	public void processResults(ResultSet rs) {
		// TODO Auto-generated method stub
		ObjectMapper mapper = new ObjectMapper();
		BulkRequestBuilder bulkRequest = elasticClient.prepareBulk();
		try {

			int rowCount = 0;

			while (rs.iterator().hasNext()) {
				Row row = rs.iterator().next();
				rowCount++;

				String text = row.getString("ibsa");

				DateFormat dateFormat = new SimpleDateFormat(
						"yyyy-MM-dd HH:mm:ss:SSSZ");
				Date date = new Date();
				String datefor = dateFormat.format(date);
				Map<String, Object> requestData = mapper.readValue(text,
						Map.class);

				requestData.put("HOST_ID", new String[] { "" });
				requestData.put("ES_CREATED_DATE", datefor);
				requestData.put("ES_UPDATED_DATE", datefor);

				if (logger.isTraceEnabled()) {
					logger.trace(mapper.writeValueAsString(requestData));
				}

				bulkRequest.add(elasticClient.prepareIndex("ibdataindexv4",
						"coveredibsa", (String) requestData.get("ID"))
						.setSource(mapper.writeValueAsString(requestData)));

			}
			if (bulkRequest.numberOfActions() > 0) {
				BulkResponse bulkResponse = bulkRequest.execute().actionGet();
				if (bulkResponse.hasFailures()) {
					for (BulkItemResponse item : bulkResponse.getItems()) {
						logger.info("problem updating content indexing for entity: "
								+ item.getId()
								+ " error: "
								+ item.getFailureMessage());
					}
				}

			}
			logger.debug("Batch Row count " + rowCount);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void close() {

	}
}
