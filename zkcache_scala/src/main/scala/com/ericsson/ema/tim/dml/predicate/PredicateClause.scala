package com.ericsson.ema.tim.dml.predicate

import com.ericsson.ema.tim.dml.{DataTypes, SelectClause}
import com.ericsson.ema.tim.exception.{DmlBadSyntaxException, DmlNoSuchFieldException}
import org.slf4j.LoggerFactory

/**
  * Created by eqinson on 2017/5/12.
  */
trait PredicateClause extends SelectClause {
	private val LOGGER = LoggerFactory.getLogger(classOf[PredicateClause])

	val valueToComp: Object

	type matcherFuncType = (Object, Object) => Boolean

	val defaultString: matcherFuncType = (_, _) => unsupportedOperation(DataTypes.String)
	val defaultInt: matcherFuncType = (_, _) => unsupportedOperation(DataTypes.Int)
	val defaultBool: matcherFuncType = (_, _) => unsupportedOperation(DataTypes.Boolean)

	val StringMatcher: matcherFuncType = defaultString
	val IntMatcher: matcherFuncType = defaultInt
	val BoolMatcher: matcherFuncType = defaultBool

	def eval(tuple: Object): Boolean = {
		if (Option(this.valueToComp).isEmpty)
			return false

		val fieldVal = getFiledValFromTupleByName(tuple)
		val fieldType = selector.context.tableMetadata.get(field)

		fieldType match {
			case Some(DataTypes.String)  => StringMatcher(fieldVal, valueToComp)
			case Some(DataTypes.Int)     => IntMatcher(fieldVal, valueToComp)
			case Some(DataTypes.Boolean) => BoolMatcher(fieldVal, valueToComp)
			case Some(other)             =>
				LOGGER.error("bug: unsupported data type: {},{}", field, other: Any)
				throw DmlBadSyntaxException("unsupported data type: " + field + "," + other)
			case None                    => throw DmlNoSuchFieldException(field)
		}
	}

	private def unsupportedOperation(oper: String): Boolean = {
		LOGGER.error("unsupported data type: {},{}", field, oper: Any)
		throw DmlBadSyntaxException("unsupported data type: " + field + "," + oper)
	}
}