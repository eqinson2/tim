package com.ericsson.ema.tim.zookeeper

import java.util.concurrent.locks.ReentrantLock

import com.ericsson.ema.tim.dml.TableInfoMap.tableInfoMap
import com.ericsson.ema.tim.reflection.Tab2ClzMap.tab2ClzMap
import com.ericsson.ema.tim.reflection.Tab2MethodInvocationCacheMap.tab2MethodInvocationCacheMap
import com.ericsson.ema.tim.zookeeper.MetaDataRegistry.metaDataRegistry
import com.ericsson.ema.tim.zookeeper.State.State
import com.ericsson.util.SystemPropertyUtil
import com.ericsson.zookeeper.{NodeChildCache, NodeChildrenChangedListener, ZooKeeperUtil}
import org.apache.zookeeper.CreateMode.PERSISTENT
import org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE
import org.apache.zookeeper.{KeeperException, WatchedEvent, Watcher, ZooKeeper}
import org.slf4j.LoggerFactory

/**
  * Created by eqinson on 2017/5/5.
  */
class ZKMonitor(private val zkConnectionManager: ZKConnectionManager) {
	private val LOGGER = LoggerFactory.getLogger(classOf[ZKMonitor])

	private var zkRootPath: String = _
	private var nodeChildCache: NodeChildCache = _
	private val lock = new ReentrantLock

	def start(): Unit = {
		try
			zkRootPath = SystemPropertyUtil.getAndAssertProperty("com.ericsson.ema.tim.zkRootPath")
		catch {
			case e: IllegalArgumentException => zkRootPath = "/TIM_POC"
		}
		zkConnectionManager.registerListener(new ZooKeeperConnectionStateListenerImpl)
		try
			ZooKeeperUtil.createRecursive(getConnection, zkRootPath, null, OPEN_ACL_UNSAFE, PERSISTENT)
		catch {
			case e@(_: KeeperException | _: InterruptedException) =>
				LOGGER.error("Failed to start ZKMonitor, the exception is ", e)
		}
		loadAllTable()
	}

	def stop(): Unit = {
		Option(nodeChildCache).foreach(_.stop)
		unloadAllTable()
	}

	private def loadAllTable(): Unit = {
		unloadAllTable()
		var children: List[String] = null
		try {
			nodeChildCache = new NodeChildCache(getConnection, zkRootPath, new NodeChildrenChangedListenerImpl)
			import scala.collection.JavaConversions._
			children = nodeChildCache.start.toList
		} catch {
			case e: KeeperException.ConnectionLossException       =>
				LOGGER.warn("Failed to setup nodeChildCache due to missing zookeeper connection.", e)
			case e@(_: KeeperException | _: InterruptedException) =>
				LOGGER.warn("Failed to loadAllTable on path: [" + zkRootPath + "]", e)
		}
		childrenAdded(children)
	}

	//thread safe
	private def loadOneTable(zkNodeName: String): Unit = {
		LOGGER.debug("Start to load data for node {}", zkNodeName)
		lock.lock()
		try {
			val rawData = zkConnectionManager.getConnection.map(getDataZKNoException(_, zkRootPath + "/" + zkNodeName, new NodeWatcher(zkNodeName)))
				.getOrElse(new Array[Byte](0))
			if (rawData.length == 0) {
				LOGGER.error("Failed to loadOneTable for node {}", zkNodeName)
				return
			}
			doLoad(zkNodeName, new String(rawData))
		} finally {
			lock.unlock()
		}
	}

	private def doLoad(tableName: String, content: String): Unit = {

	}

	private def getDataZKNoException(zooKeeper: ZooKeeper, zkTarget: String, watcher: Watcher) =
		try
			zooKeeper.getData(zkTarget, watcher, null)
		catch {
			case e@(_: KeeperException | _: InterruptedException) =>
				LOGGER.warn("Failed to get data from " + zkTarget, e)
				new Array[Byte](0)
		}

	private def getConnection = zkConnectionManager.getConnection.getOrElse(throw new KeeperException.ConnectionLossException)

	private def unloadAllTable() = {
		LOGGER.info("=====================unregister all table=====================")
		metaDataRegistry.clear()
		tableInfoMap.clear()
		tab2MethodInvocationCacheMap.clear()
		tab2ClzMap.clear()
	}

	private def unloadOneTable(zkNodeName: String) = {
		LOGGER.info("=====================registerOrReplace {}=====================", zkNodeName)
		metaDataRegistry.unregisterMetaData(zkNodeName)
		tableInfoMap.unregister(zkNodeName)
		tab2MethodInvocationCacheMap.unRegister(zkNodeName)
		tab2ClzMap.unRegister(zkNodeName)
	}

	private def childrenAdded(children: List[String]): Unit = {
		children.foreach(loadOneTable)
	}

	private def childrenRemoved(children: List[String]): Unit = {
		children.foreach(unloadOneTable)
	}

	private class NodeChildrenChangedListenerImpl extends NodeChildrenChangedListener {

		import scala.collection.JavaConversions._

		override def childAdded(children: java.util.List[String]): Unit = {
			childrenAdded(children.toList)
		}

		override def childRemoved(children: java.util.List[String]): Unit = {
			childrenRemoved(children.toList)
		}

		override def terminallyFailed(): Unit = {
			LOGGER.error("Unexpected failure happens!")
		}
	}

	private class ZooKeeperConnectionStateListenerImpl extends ZKConnectionChangeWatcher {
		override def stateChange(state: State): Unit = {
			state match {
				case State.CONNECTED | State.RECONNECTED => loadAllTable()
				case State.DISCONNECTED                  => Option(nodeChildCache).foreach(_.stop)
			}
		}

	}

	private class NodeWatcher private[zookeeper](val zkNodeName: String) extends Watcher {
		override def process(event: WatchedEvent): Unit = {
			if (event.getType eq Watcher.Event.EventType.NodeDataChanged)
				loadOneTable(zkNodeName)
		}
	}

}


