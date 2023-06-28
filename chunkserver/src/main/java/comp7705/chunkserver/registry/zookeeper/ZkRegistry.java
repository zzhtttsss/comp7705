package comp7705.chunkserver.registry.zookeeper;

import comp7705.chunkserver.common.Const;
import comp7705.chunkserver.entity.URL;
import comp7705.chunkserver.registry.AbstractRegistry;

import java.util.ArrayList;
import java.util.List;


/**
 * @author Reuze
 * @Date 26/05/2023
 */
public class ZkRegistry extends AbstractRegistry {

    private final CuratorZkClient zkClient;

    public ZkRegistry() {
        zkClient = new CuratorZkClient(Const.ZkIp + ":" + Const.ZkPort);
    }

    @Override
    protected void doRegister(URL url) {
        zkClient.createEphemeralNode(url.getZkPath());
        watch(url);
    }

    @Override
    protected void doUnregister(URL url) {
        zkClient.removeNode(url.getZkPath());
        watch(url);
    }

    @Override
    protected URL doLookup(URL condition) {
        List<String> children = zkClient.getChildren(condition.getZkPath());
        List<URL> urls = new ArrayList<>(children.size());
        for (String child : children) {
            URL url = new URL();
            url.setIp(child.split(":")[0]);
            url.setPort(Integer.parseInt(child.split(":")[1]));
            url.setPath(condition.getPath());
            urls.add(url);
        }
        for (URL url : urls) {
            watch(url);
        }
        return urls.size() == 0 ? null : urls.get(0);
    }

    private void watch(URL url) {
        String path = url.getZkPath();
        zkClient.addListener(path, ((type, oldData, data) -> {
            reset(url);
        }));
    }
}
