package com.ericsson.ema.tim.reflection


import java.beans.Introspector
import java.lang.reflect.InvocationTargetException

import com.ericsson.ema.tim.dml.DataTypes
import com.ericsson.ema.tim.exception.DmlNoSuchFieldException
import com.ericsson.ema.tim.json.{FieldInfo, JsonLoader}
import com.ericsson.ema.tim.reflection.Tab2ClzMap.tab2ClzMap
import com.ericsson.ema.tim.reflection.Tab2MethodInvocationCacheMap.tab2MethodInvocationCacheMap
import org.slf4j.LoggerFactory

/**
  * Created by eqinson on 2017/5/10.
  */
class TabDataLoader(val classToLoad: String, val jloader: JsonLoader) {
	private val LOGGER = LoggerFactory.getLogger(classOf[TabDataLoader])

	private val TUPLE_FIELD = "records"
	private val cache = tab2MethodInvocationCacheMap.lookup(jloader.tableName)

	private def realFieldVal(field: FieldInfo): Object = {
		var value: Object = null
		field.fieldType match {
			case DataTypes.String  => field.fieldValue
			case DataTypes.Int     => java.lang.Integer.valueOf(field.fieldValue)
			case DataTypes.Boolean => java.lang.Boolean.valueOf(field.fieldValue)
			case _                 =>
				LOGGER.error("unsupported data type: {}", field.fieldValue)
				throw DmlNoSuchFieldException(field.fieldName)
		}
	}

	def loadData: Object = {
		LOGGER.info("=====================reflect class: {}=====================", classToLoad)
		val clz = tab2ClzMap.lookup(jloader.tableName).getOrElse(Thread.currentThread.getContextClassLoader.loadClass(classToLoad))
		tab2ClzMap.register(jloader.tableName, clz)
		val obj = clz.newInstance
		val tupleListType = loadTupleClz(obj)
		LOGGER.debug("init {}", tupleListType)
		val getter = cache.get(clz, TUPLE_FIELD, AccessType.GET)
		val records = getter.invoke(obj).asInstanceOf[java.util.List[Object]]
		for (row <- jloader.tupleList) {
			val tuple = tupleListType.newInstance.asInstanceOf[Object]
			row.foreach(field => fillinField(tuple, field, realFieldVal(field)))
			records.add(tuple)
		}
		obj.asInstanceOf[Object]
	}

	private def fillinField(tuple: Object, field: FieldInfo, value: Object): Unit = {
		val beanInfo = Introspector.getBeanInfo(tuple.getClass)
		val propertyDescriptors = beanInfo.getPropertyDescriptors
		propertyDescriptors.toList.filter(field.fieldName == _.getName) match {
			case h :: _ =>
				val setter = h.getWriteMethod
				try {
					setter.invoke(tuple, value)
					LOGGER.debug("fillinField : {} = {}", field.fieldName, value: Any)
				} catch {
					case e@(_: IllegalAccessException | _: InvocationTargetException) =>
						e.printStackTrace()
						LOGGER.error("error fillinField : {}", field)
				}
			case _      =>
				LOGGER.error("should not happen.")
				throw new RuntimeException("bug in fillinField...")
		}
	}

	private def loadTupleClz(instance: Any): Class[_] = {
		val tupleClassName = instance.getClass.getName + "Data"
		//must use same classloader as PojoGen
		LOGGER.info("=====================load class: {}=====================", tupleClassName)
		instance.getClass.getClassLoader.loadClass(tupleClassName)
	}
}

object TabDataLoader {
	def apply(classToLoad: String, jloader: JsonLoader) = new TabDataLoader(classToLoad, jloader)
}
