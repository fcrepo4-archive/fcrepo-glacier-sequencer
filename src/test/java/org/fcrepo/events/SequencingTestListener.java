package org.fcrepo.events;

import static org.modeshape.jcr.api.observation.Event.Sequencing.NODE_SEQUENCED;
import static org.modeshape.jcr.api.observation.Event.Sequencing.OUTPUT_PATH;
import static org.modeshape.jcr.api.observation.Event.Sequencing.SELECTED_PATH;
import static org.modeshape.jcr.api.observation.Event.Sequencing.SEQUENCED_NODE_ID;
import static org.modeshape.jcr.api.observation.Event.Sequencing.SEQUENCED_NODE_PATH;
import static org.modeshape.jcr.api.observation.Event.Sequencing.SEQUENCER_NAME;
import static org.modeshape.jcr.api.observation.Event.Sequencing.USER_ID;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.observation.EventIterator;
import javax.jcr.observation.EventListener;

import org.modeshape.jcr.api.observation.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SequencingTestListener implements EventListener {
	private static Logger LOG = LoggerFactory.getLogger(SequencingTestListener.class);
	private ConcurrentHashMap<String, Event> sequencingEvents;
    private final Map<String, Node> sequencedNodes = new HashMap<String, Node>();
	
    public SequencingTestListener(ConcurrentHashMap<String, Event> sequencingEvents) {
    	this.sequencingEvents = sequencingEvents;
    }
    
	@Override
	public void onEvent(EventIterator events) {
        while (events.hasNext()) {
            try {
                Event event = (Event)events.nextEvent();
                LOG.info("Received event: " + event.toString());
                Map info = event.getInfo();
                for (Object key: info.keySet()){
                	LOG.debug("EVENT INFO: " + key + " => " + info.get(key));
                }
                sequencingEvents.putIfAbsent((String)event.getInfo().get(SEQUENCED_NODE_PATH), event);

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
	
	}

}
