package com.ericsson.ema.tim.dml

import java.lang.reflect.InvocationTargetException

import com.ericsson.ema.tim.context.{Tab2MethodInvocationCacheMap, TableInfoContext, TableInfoMap}
import com.ericsson.ema.tim.exception.DmlBadSyntaxException
import com.ericsson.ema.tim.reflection.{AccessType, MethodInvocationCache}

/**
  * Created by eqinson on 2017/6/23.
  */
abstract class Operator {
	private val TUPLE_FIELD = "records"
	protected var table: String = _
	protected var records: List[Object] = _

	var context: TableInfoContext = _
	var methodInvocationCache: MethodInvocationCache = _

	protected def initExecuteContext(): Unit = {
		this.context = TableInfoMap().lookup(table).getOrElse(throw DmlBadSyntaxException("Error: Selecting a " + "non-existing table:" + table))
		this.methodInvocationCache = Tab2MethodInvocationCacheMap().lookup(table)
		//it is safe because records must be List according to JavaBean definition
		val tupleField = invokeGetByReflection(context.tabledata, TUPLE_FIELD)
		import scala.collection.JavaConversions._
		this.records = tupleField.asInstanceOf[java.util.List[Object]].toList
	}

	protected def invokeGetByReflection(obj: Object, wantedField: String): Object = {
		val getter = methodInvocationCache.get(obj.getClass, wantedField, AccessType.GET)
		try
			getter.invoke(obj)
		catch {
			case e@(_: IllegalAccessException | _: InvocationTargetException) =>
				throw DmlBadSyntaxException(e.getMessage) //should never happen
		}
	}

	protected def invokeSetByReflection(obj: Object, wantedField: String, newValue: Object): Unit = {
		val setter = methodInvocationCache.get(obj.getClass, wantedField, AccessType.SET)
		try
			setter.invoke(obj, newValue)
		catch {
			case e@(_: IllegalAccessException | _: InvocationTargetException) =>
				throw DmlBadSyntaxException(e.getMessage) //should never happen
		}
	}
}
