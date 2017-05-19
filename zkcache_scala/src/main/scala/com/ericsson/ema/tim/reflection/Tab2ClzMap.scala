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
	var instance: Tab2ClzMap = _

	def tab2ClzMap: Tab2ClzMap = synchronized {
		Option(instance) match {
			case None    =>
				instance = new Tab2ClzMap
				instance
			case Some(_) => instance
		}
	}
}

