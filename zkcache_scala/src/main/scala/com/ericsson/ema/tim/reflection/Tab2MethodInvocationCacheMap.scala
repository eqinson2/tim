package com.ericsson.ema.tim.reflection

/**
  * Created by eqinson on 2017/5/5.
  */
class Tab2MethodInvocationCacheMap {
	private var map = Map[String, MethodInvocationCache]()

	def clear(): Unit = {
		map = Map[String, MethodInvocationCache]()
	}

	def unRegister(tableName: String): Unit = {
		map.get(tableName).foreach(_.cleanup())
		map -= tableName
	}

	def lookup(tableName: String): MethodInvocationCache = {
		map.get(tableName) match {
			case Some(cache) => cache
			case None        =>
				val cache = new MethodInvocationCache()
				map += (tableName -> cache)
				cache
		}
	}

}

object Tab2MethodInvocationCacheMap {
	var instance: Tab2MethodInvocationCacheMap = _

	def tab2MethodInvocationCacheMap: Tab2MethodInvocationCacheMap = synchronized {
		if (instance == null)
			instance = new Tab2MethodInvocationCacheMap
		instance
	}
}
