package com.ericsson.ema.tim.zookeeper;

import com.ericsson.util.SystemPropertyUtil;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.ericsson.ema.tim.zookeeper.ZKConnectionManager.zkConnectionManager;

public class ZKPersistenceUtil {
    private final static Logger LOGGER = LoggerFactory.getLogger(ZKPersistenceUtil.class);
    private static String zkRootPath = "/TIM_POC";

    static {
        try {
            zkRootPath = SystemPropertyUtil.getAndAssertProperty("com.ericsson.ema.tim.zkRootPath");
        } catch (IllegalArgumentException e) {
            zkRootPath = "/TIM_POC";
        }
    }

    private final ZKConnectionManager zkm = zkConnectionManager;

    private ZooKeeper getConnection() throws KeeperException.ConnectionLossException {
        return zkm.getConnection().orElseThrow(KeeperException.ConnectionLossException::new);
    }

    private boolean exists(String path) {
        try {
            return getConnection().exists(path, false) != null;
        } catch (Exception e) {
            LOGGER.error("{}", e);
        }
        return false;
    }

    public void persist(String table, String data) throws KeeperException.ConnectionLossException {
        String tabPath = zkRootPath + "/" + table;
        if (!exists(tabPath)) {
            throw new RuntimeException("root path " + zkRootPath + "does not exist!");
        } else {
            LOGGER.debug("set znode {}" + tabPath);
            setNodeData(tabPath, data);
        }

        if (LOGGER.isDebugEnabled())
            LOGGER.debug("{} data: {}", tabPath, getNodeData(tabPath));
    }

    private void setNodeData(String path, String data) throws KeeperException.ConnectionLossException {
        if (isConnected()) {
            try {
                getConnection().setData(path, data.getBytes(), -1);
            } catch (Exception e) {
                LOGGER.error("{}", e);
            }
        }
    }

    private String getNodeData(String path) throws KeeperException.ConnectionLossException {
        if (isConnected()) {
            try {
                byte[] byteData = getConnection().getData(path, true, null);
                return new String(byteData, "utf-8");
            } catch (Exception e) {
                e.printStackTrace();
                LOGGER.error("{}", e);
                return null;
            }
        }
        return null;
    }

    private boolean isConnected() throws KeeperException.ConnectionLossException {
        return getConnection().getState().isConnected();
    }
}