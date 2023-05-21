package comp7705.chunkserver.registry;

import comp7705.chunkserver.entity.URL;

/**
 * @author Reuze
 * @Date 26/05/2023
 */
public interface Registry {

    void register(URL url);

    void unregister(URL url);

    URL lookup(String condition);
}
