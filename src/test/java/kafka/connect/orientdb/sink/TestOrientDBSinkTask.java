package kafka.connect.orientdb.sink;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import kafka.connect.beans.Account;
import kafka.connect.beans.Client;
import kafka.connect.beans.QuoteRequest;
import kafka.connect.orientdb.Writer;
import mockit.Deencapsulation;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Tested;
import mockit.Verifications;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class TestOrientDBSinkTask {
	
	@Tested
	private OrientDBSinkTask orientDBSinkTask;
	
	@Injectable
	private Writer writer;
	
	@Injectable
	private SinkTaskContext sinkTaskContext;
	
	private final Map<String, String> conf = new HashMap<>();
	private final List<SinkRecord> documents = new ArrayList<SinkRecord>();
	private static final ObjectMapper MAPPER = new ObjectMapper();
	
	@Before
    public void setup(){

        conf.put(OrientDBSinkConfig.BATCH_SIZE, "100");
        conf.put(OrientDBSinkConfig.RETRY_BACKOFF_MS, "100");
        conf.put(OrientDBSinkConfig.MAX_RETRIES, "3");
        conf.put(OrientDBSinkConfig.CONNECTION_STRING, "remote:localhost/kafka-connect-orientdb");
        conf.put(OrientDBSinkConfig.CONNECTION_USER, "test");
        conf.put(OrientDBSinkConfig.CONNECTION_PASSWORD, "test");
    }

	@Test
	public void shouldPutRecordsInTheWriter() {
		
		orientDBSinkTask.start(conf);
		initDependencies(); 
		
		new Expectations() {{
			writer.write(documents);
			times = 1;
		}};
		
		final Account account = new Account("A1");
		final Client client = new Client("C1", account);
		final QuoteRequest quoteRequest = new QuoteRequest("Q1", "APPL", 100, client, new Date());
		documents.add(new SinkRecord("trades", 1, null, null, null,  MAPPER.convertValue(quoteRequest, Map.class), 0));
		orientDBSinkTask.put(documents);
		
		new Verifications() {{
			List<SinkRecord> ds;
			writer.write(ds = withCapture());
			assertEquals("trades", ds.get(0).topic());
			assertEquals(1, ds.size());
		}};
	}
	
	@Test(expected = ConnectException.class)
	public void testRetryCount() {
		
		orientDBSinkTask.start(conf);
		
		new Expectations() {{
			writer.write(documents);
			times = 4;
			result = new RetriableException("A RetriableException Test Exception!");
		}};
		
		final Account account = new Account("A1");
		final Client client = new Client("C1", account);
		final QuoteRequest quoteRequest = new QuoteRequest("Q1", "APPL", 100, client, new Date());
		documents.add(new SinkRecord("trades", 1, null, null, null,  MAPPER.convertValue(quoteRequest, Map.class), 0));
		try {
	        initDependencies();
	        orientDBSinkTask.put(documents);
		} catch (RetriableException e) {
			assertEquals(RetriableException.class.getName(), e.getClass().getName());
	        initDependencies();
			try {
				orientDBSinkTask.put(documents);
			} catch (Exception e1) {
				assertEquals(RetriableException.class.getName(), e.getClass().getName());
		        initDependencies();
		        try {
		        		orientDBSinkTask.put(documents);
				} catch (Exception e2) {
					assertEquals(RetriableException.class.getName(), e.getClass().getName());
			        initDependencies();
			        orientDBSinkTask.put(documents);
				}
			}
		}
	}
	
	
	private void initDependencies() {
        Deencapsulation.setField(orientDBSinkTask, writer);
        Deencapsulation.setField(orientDBSinkTask, sinkTaskContext);
	}

}
