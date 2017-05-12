package com.ericsson.ema.tim.dml

import java.lang.reflect.InvocationTargetException

import com.ericsson.ema.tim.exception.DmlBadSyntaxException
import com.ericsson.ema.tim.reflection.AccessType

/**
  * Created by eqinson on 2017/5/12.
  */
trait SelectClause {
	var selector: Select = _

	def getField: String

	protected def getFiledValFromTupleByName(tuple: Object): Object = {
		val getter = selector.methodInvocationCache.get(tuple.getClass, getField, AccessType.GET)
		try
			getter.invoke(tuple)
		catch {
			case e@(_: IllegalAccessException | _: InvocationTargetException) =>
				throw DmlBadSyntaxException(e.getMessage) //should never happen

		}
	}
}
