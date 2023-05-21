package comp7705.chunkserver.registry;

import comp7705.chunkserver.entity.URL;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Reuze
 * @Date 26/05/2023
 */
public abstract class AbstractRegistry implements Registry {

    private final Map<String, URL> registered = new ConcurrentHashMap<>();

    protected abstract void doRegister(URL url);

    protected abstract void doUnregister(URL url);

    protected abstract URL doLookup(URL condition);

    @Override
    public void register(URL url) {
        doRegister(url);
        addToLocalCache(url);
    }

    @Override
    public void unregister(URL url) {
        doUnregister(url);
        removeFromLocalCache(url.getPath());
    }

    @Override
    public URL lookup(String condition) {
        if (registered.containsKey(condition)) {
            return registered.get(condition);
        }
        URL url = new URL();
        url.setPath(condition);
        return reset(url);
    }

    public URL reset(URL condition) {
        URL url = doLookup(condition);
        addToLocalCache(url);
        return url;
    }

    private void addToLocalCache(URL url) {
        registered.put(url.getPath(), url);
    }

    private void removeFromLocalCache(String condition) {
        if (registered.containsKey(condition)) {
            registered.remove(condition);
        }
    }

}
