package kafka.connect.orientdb;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;


/**
 * 
 * @author Sanju Thomas
 *
 */
public class OrientDBWriter implements Writer{
	
	private static final Logger logger = LoggerFactory.getLogger(OrientDBWriter.class);
	private static final ObjectMapper MAPPER = new ObjectMapper();
	
	public OrientDBWriter(final Map<String, String> config){
	    
	}
	

    @Override
    public void write(final Collection<SinkRecord> records) {
    	
    	
    }
}
