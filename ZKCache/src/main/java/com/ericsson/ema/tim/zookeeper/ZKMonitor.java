package com.ericsson.ema.tim.zookeeper;

import com.ericsson.ema.tim.json.JsonLoader;
import com.ericsson.ema.tim.pojo.PojoGenerator;
import com.ericsson.ema.tim.pojo.model.NameType;
import com.ericsson.ema.tim.pojo.model.Table;
import com.ericsson.ema.tim.pojo.model.TableTuple;
import com.ericsson.ema.tim.reflection.TabDataLoader;
import com.ericsson.util.SystemPropertyUtil;
import com.ericsson.zookeeper.NodeChildCache;
import com.ericsson.zookeeper.NodeChildrenChangedListener;
import com.ericsson.zookeeper.ZooKeeperUtil;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.ericsson.ema.tim.dml.TableInfoMap.tableInfoMap;
import static com.ericsson.ema.tim.lock.ZKCacheRWLockMap.zkCacheRWLock;
import static com.ericsson.ema.tim.reflection.Tab2ClzMap.tab2ClzMap;
import static com.ericsson.ema.tim.reflection.Tab2MethodInvocationCacheMap.tab2MethodInvocationCacheMap;
import static com.ericsson.ema.tim.zookeeper.MetaDataRegistry.metaDataRegistry;
import static org.apache.zookeeper.CreateMode.PERSISTENT;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class ZKMonitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZKMonitor.class);

    private static String zkRootPath = "/TIM_POC";

    private final ZKConnectionManager zkConnectionManager;
    private NodeChildCache nodeChildCache;

    public ZKMonitor(ZKConnectionManager zkConnectionManager) {
        this.zkConnectionManager = zkConnectionManager;
    }

    public void start() {
        try {
            zkRootPath = SystemPropertyUtil.getAndAssertProperty("com.ericsson.ema.tim.zkRootPath");
        } catch (IllegalArgumentException e) {
            zkRootPath = "/TIM_POC";
        }

        zkConnectionManager.registerListener(new ZooKeeperConnectionStateListenerImpl());
        try {
            ZooKeeperUtil.createRecursive(getConnection(), zkRootPath, null, OPEN_ACL_UNSAFE, PERSISTENT);
        } catch (KeeperException | InterruptedException e) {
            LOGGER.error("Failed to start ZKMonitor, the exception is ", e);
        }
        loadAllTable();
    }

    public void stop() {
        Optional.ofNullable(nodeChildCache).ifPresent(NodeChildCache::stop);
        unloadAllTable();
    }

    private void loadAllTable() {
        unloadAllTable();

        List<String> children = new ArrayList<>();
        try {
            nodeChildCache = new NodeChildCache(getConnection(), zkRootPath, new NodeChildrenChangedListenerImpl());
            children = nodeChildCache.start();
        } catch (KeeperException.ConnectionLossException e) {
            LOGGER.warn("Failed to setup nodeChildCache due to missing zookeeper connection.", e);
        } catch (KeeperException | InterruptedException e) {
            LOGGER.warn("Failed to loadAllTable on path: [" + zkRootPath + "]", e);
        }

        childrenAdded(children);
    }

    //thread safe
    private synchronized void loadOneTable(String zkNodeName) {
        LOGGER.debug("Start to load data for node {}", zkNodeName);
        byte[] rawData = zkConnectionManager.getConnection()
                .map(zkConnection -> getDataZKNoException(zkConnection, zkRootPath + "/" + zkNodeName, new NodeWatcher
                        (zkNodeName))).orElse(new byte[0]);

        if (rawData.length == 0) {
            LOGGER.error("Failed to loadOneTable for node {}", zkNodeName);
            return;
        }

        doLoad(zkNodeName, new String(rawData));
    }

    public void doLoad(String tableName, String content) {
        //1. load json
        JsonLoader jloader = loadJsonFromRawData(content, tableName);
        boolean needToInvalidateInvocationCache = false;
        if (!isMetaDataDefined(jloader)) {
            //metadata change-> function need re-reflection
            tab2ClzMap.unRegister(tableName);
            needToInvalidateInvocationCache = true;

            //2. parse json cache and build as datamodel
            Table table = buildDataModelFromJson(jloader);
            //3. generate pojo class
            PojoGenerator.generateTableClz(table);
            updateMetaData(jloader);
        }
        //4. load data by reflection, and the new data will replace old one.
        Object obj = loadDataByReflection(jloader);

        //8. registerOrReplace tab into global registry
        LOGGER.info("=====================registerOrReplace {}=====================", tableName);
        //force original loaded obj and its classloader to gc
        zkCacheRWLock.writeLockTable(tableName);
        try {
            if (needToInvalidateInvocationCache)
                tab2MethodInvocationCacheMap.unRegister(tableName);
            tableInfoMap.registerOrReplace(tableName, jloader.getTableMetadata(), obj);
        } finally {
            zkCacheRWLock.writeUnLockTable(tableName);
        }
    }

    private boolean isMetaDataDefined(JsonLoader jsonLoader) {
        boolean defined = metaDataRegistry.isRegistered(jsonLoader.getTableName(), jsonLoader.getTableMetadata());
        if (defined) {
            LOGGER.info("Metadata already defined for {}, skip regenerating javabean...", jsonLoader
                    .getTableName());
        } else {
            LOGGER.info("Metadata NOT defined for {}", jsonLoader.getTableName());
        }
        return defined;
    }

    private void updateMetaData(JsonLoader jsonLoader) {
        metaDataRegistry.registerMetaData(jsonLoader.getTableName(), jsonLoader.getTableMetadata());
    }

    private JsonLoader loadJsonFromRawData(String json, String tableName) {
        JsonLoader jloader = new JsonLoader(tableName);
        jloader.loadJsonFromString(json);
        return jloader;
    }

    private Table buildDataModelFromJson(JsonLoader jloader) {
        LOGGER.info("=====================parse json=====================");
        TableTuple tt = new TableTuple("records", jloader.getTableName() + "Data");
        jloader.getTableMetadata().forEach((k, v) -> tt.getTuples().add(new NameType(k, v)));
        Table table = new Table(jloader.getTableName(), tt);

        LOGGER.debug("Table structure: {}", table);
        return table;
    }

    private Object loadDataByReflection(JsonLoader jloader) {
        LOGGER.info("=====================load data by reflection=====================");
        String classToLoad = PojoGenerator.pojoPkg + "." + jloader.getTableName();
        TabDataLoader tabL = new TabDataLoader(classToLoad, jloader);
        try {
            return tabL.loadData();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException |
                InvocationTargetException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    private void unloadAllTable() {
        LOGGER.info("=====================unregister all table=====================");
        metaDataRegistry.clear();
        tableInfoMap.clear();
        tab2MethodInvocationCacheMap.clear();
        tab2ClzMap.clear();
    }

    private void unloadOneTable(String zkNodeName) {
        LOGGER.info("=====================registerOrReplace {}=====================", zkNodeName);
        metaDataRegistry.unregisterMetaData(zkNodeName);
        tableInfoMap.unregister(zkNodeName);
        tab2MethodInvocationCacheMap.unRegister(zkNodeName);
        tab2ClzMap.unRegister(zkNodeName);
    }

    private void childrenAdded(List<String> children) {
        children.forEach(this::loadOneTable);
    }

    private void childrenRemoved(List<String> children) {
        children.forEach(this::unloadOneTable);
    }

    private ZooKeeper getConnection() throws KeeperException.ConnectionLossException {
        return zkConnectionManager.getConnection().orElseThrow(KeeperException
                .ConnectionLossException::new);
    }

    private byte[] getDataZKNoException(ZooKeeper zooKeeper, String zkTarget, Watcher watcher) {
        try {
            return zooKeeper.getData(zkTarget, watcher, null);
        } catch (KeeperException | InterruptedException e) {
            LOGGER.warn("Failed to get data from " + zkTarget, e);
            return new byte[0];
        }
    }

    private class NodeChildrenChangedListenerImpl implements NodeChildrenChangedListener {
        @Override
        public void childAdded(List<String> children) {
            childrenAdded(children);
        }

        @Override
        public void childRemoved(List<String> children) {
            childrenRemoved(children);
        }

        /**
         * Notifies that we failed to read the data from the path and registerOrReplace a new watcher. <br>
         * The only way out of this is to restart the {@link NodeChildCache} instance.
         *
         * @since 2.8
         */
        @Override
        public void terminallyFailed() {
            LOGGER.error("Unexpected failure happens!");
        }
    }

    private class ZooKeeperConnectionStateListenerImpl implements ZKConnectionChangeWatcher {
        @Override
        public void stateChange(State state) {
            if (state.equals(State.CONNECTED) || state.equals(State.RECONNECTED)) {
                loadAllTable();
            } else if (state.equals(State.DISCONNECTED)) {
                Optional.ofNullable(nodeChildCache).ifPresent(NodeChildCache::stop);
            }
        }
    }

    private class NodeWatcher implements Watcher {
        private final String zkNodeName;

        NodeWatcher(String zkNodeName) {
            this.zkNodeName = zkNodeName;
        }

        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
                loadOneTable(zkNodeName);
            }
        }
    }
}
