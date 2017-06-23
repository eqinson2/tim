package com.ericsson.ema.tim.zookeeper

import com.ericsson.util.SystemPropertyUtil
import org.apache.zookeeper.KeeperException
import org.slf4j.LoggerFactory

/**
  * Created by eqinson on 2017/6/23.
  */
object ZKPersistenceUtil {
	private[this] val LOGGER = LoggerFactory.getLogger(ZKPersistenceUtil.getClass)
	private[this] val zkRootPath = SystemPropertyUtil.getAndAssertProperty("com.ericsson.ema.tim.zkRootPath")

	private[this] val zkm = ZKConnectionManager()

	private[this] def getConnection = zkm.getConnection.getOrElse(throw new KeeperException.ConnectionLossException)

	private[this] def isConnected = getConnection.getState.isConnected

	private[this] def exists(path: String): Boolean = {
		try
			getConnection.exists(path, false) != null
		catch {
			case e: Exception => LOGGER.error("{}", e)
		}
		false
	}

	private[this] def setNodeData(path: String, data: String) = {
		if (isConnected)
			try
				getConnection.setData(path, data.getBytes, -1)
			catch {
				case e: Exception => LOGGER.error("{}", e)
			}
	}

	private def getNodeData(path: String) = {
		if (isConnected)
			try {
				val byteData = getConnection.getData(path, true, null)
				new String(byteData, "utf-8")
			} catch {
				case e: Exception => LOGGER.error("{}", e); null
			}
		else null
	}

	def persist(table: String, data: String): Unit = {
		val tabPath = zkRootPath + "/" + table
		if (!exists(tabPath))
			throw new RuntimeException("root path " + zkRootPath + "does not exist!")
		else {
			LOGGER.debug("set znode {}" + tabPath)
			setNodeData(tabPath, data)
		}
		if (LOGGER.isDebugEnabled)
			LOGGER.debug("{} data: {}", tabPath, getNodeData(tabPath): Any)
	}

}
