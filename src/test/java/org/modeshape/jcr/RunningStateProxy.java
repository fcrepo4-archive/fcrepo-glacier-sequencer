package org.modeshape.jcr;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.modeshape.jcr.JcrRepository.RunningState;
import org.modeshape.jcr.api.sequencer.Sequencer;

public class RunningStateProxy {
    public static RunningState getRunningState(JcrRepository repo) {
    	return repo.runningState();
    }
    
    private final RunningState state;
    public RunningStateProxy(JcrRepository repo){
    	this.state = repo.runningState();
    }
    public Sequencers getSequencers(){
    	Sequencers result = state.sequencers();
    	result.getClass();
    	return result;
    }
    
    public <T extends Sequencer>List<T> getSequencersByType(Class<T> clazz) {
		ArrayList<T> result = new ArrayList<T>();
    	try {
			Field sequencers = Sequencers.class.getDeclaredField("sequencersById");
			sequencers.setAccessible(true);
			Object uncast = sequencers.get(getSequencers());
			Map<UUID, Sequencer> map = (Map)uncast;
			for(Sequencer s:map.values()){
				if (clazz.isAssignableFrom(s.getClass())){
					result.add((T)s);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
    }
}
