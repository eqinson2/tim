package com.ericsson.ema.tim.reflection

import org.slf4j.LoggerFactory

/**
  * Created by eqinson on 2017/5/8.
  */
class JavaBeanReflectionProxy(val instance: Any) {
	private val TUPLENAME = "records"
	private val LOGGER = LoggerFactory.getLogger(classOf[JavaBeanReflectionProxy])
	var tupleListType: Class[_] = _

	def init(): Unit = {
		val tupleClassName = instance.getClass.getName + "Data"
		//must use same classloader as PojoGen
		val cl = instance.getClass.getClassLoader
		this.tupleListType = if (cl ne null) cl.loadClass(tupleClassName) else Class.forName(tupleClassName)
	}

}
