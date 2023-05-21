package comp7705.chunkserver.registry.zookeeper;

import comp7705.chunkserver.common.Const;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author Reuze
 * @Date 26/05/2023
 */
public class CuratorZkClient {


    private final CuratorFramework client;

    private static final Map<String, CuratorCache> LISTENER_MAP = new ConcurrentHashMap<>();

    public CuratorZkClient(String zkAddr) {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
                .connectString(zkAddr)
                .retryPolicy(new RetryNTimes(Const.RETRY_TIMES, Const.RETRY_SLEEP_MS))
                .connectionTimeoutMs(Const.DEFAULT_CONNECTION_TIMEOUT_MS)
                .sessionTimeoutMs(Const.DEFAULT_SESSION_TIMEOUT_MS);
        client = builder.build();
        client.start();
        try {
            client.blockUntilConnected(Const.DEFAULT_CONNECTION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void createNode(String path, CreateMode createMode) {
        try {
            client.create().creatingParentsIfNeeded().withMode(createMode).forPath(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createEphemeralNode(String path) {
        createNode(path, CreateMode.EPHEMERAL);
    }

    public void createPersistentNode(String path) {
        createNode(path, CreateMode.PERSISTENT);
    }

    public void removeNode(String path) {
        try {
            client.delete().forPath(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<String> getChildren(String path) {
        try {
            return client.getChildren().forPath(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    public void addListener(String path, CuratorCacheListener listener) {
        if (LISTENER_MAP.containsKey(path)) {
            return;
        }
        CuratorCache curatorCache = CuratorCache.build(client, path);
        LISTENER_MAP.put(path, curatorCache);
        curatorCache.listenable().addListener(listener);
        curatorCache.start();
    }

}
