package com.ericsson.ema.tim.zookeeper

/**
  * Created by eqinson on 2017/5/5.
  */
class MetaDataRegistry {
	private var registry = Map[String, Map[String, String]]()

	def registerMetaData(tableName: String, metadata: Map[String, String]): Unit = {
		registry += (tableName -> metadata)
	}

	def unregisterMetaData(tableName: String): Unit = {
		registry -= tableName
	}

	def clear(): Unit = {
		registry = Map[String, Map[String, String]]()
	}

	def isRegistered(tableName: String, other: Map[String, String]): Boolean = {
		registry.exists(_._1 == tableName) && registry.get(tableName) == other
	}

}

object MetaDataRegistry {
	var instance: MetaDataRegistry = _

	def metaDataRegistry: MetaDataRegistry = synchronized {
		if (instance == null)
			instance = new MetaDataRegistry
		instance
	}
}
