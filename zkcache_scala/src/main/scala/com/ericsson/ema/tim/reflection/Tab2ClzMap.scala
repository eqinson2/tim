package com.ericsson.ema.tim.reflection

/**
  * Created by eqinson on 2017/5/5.
  */
class Tab2ClzMap {
	private var registry = Map[String, Class[_]]()

	def register(tableName: String, clz: Class[_]): Unit = {
		registry += (tableName -> clz)
	}

	def unRegister(tableName: String): Unit = {
		registry -= tableName
	}

	def clear(): Unit = {
		registry = Map[String, Class[_]]()
	}

	def lookup(tableName: String): Option[Class[_]] = registry.get(tableName)
}

object Tab2ClzMap {
	var instance: Tab2ClzMap = new Tab2ClzMap

	def apply(): Tab2ClzMap = instance
}

