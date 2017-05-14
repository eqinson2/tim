package com.ericsson.ema.tim.dml.predicate

import com.ericsson.ema.tim.dml.DataTypes
import com.ericsson.ema.tim.exception.{DmlBadSyntaxException, DmlNoSuchFieldException}
import org.slf4j.LoggerFactory

/**
  * Created by eqinson on 2017/5/13.
  */
class BiggerThan private(override val field: String, override val valueToComp: Object) extends AbstractPredicate(field,
	valueToComp) with Predicate {
	private val LOGGER = LoggerFactory.getLogger(classOf[BiggerThan])

	override def eval(tuple: Object): Boolean = {
		if (this.valueToComp eq null)
			return false

		val fieldVal: Object = getFiledValFromTupleByName(tuple)
		val fieldType = selector.context.tableMetadata.get(field)
		fieldType match {
			case Some(DataTypes.Int) =>
				fieldVal.asInstanceOf[Integer].compareTo(valueToComp.asInstanceOf[Integer]) > 0
			case Some(other)         =>
				LOGGER.error("unsupported data type: {},{}", field, other: Any)
				throw DmlBadSyntaxException("unsupported data type: " + field + "," + other)
			case None                => throw DmlNoSuchFieldException(field)
		}
	}
}

object BiggerThan {
	def apply(field: String, value: Int) = new BiggerThan(field, Integer.valueOf(value))
}

