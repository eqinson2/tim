package com.ericsson.ema.tim.dml.predicate

import com.ericsson.ema.tim.dml.DataTypes
import com.ericsson.ema.tim.exception.{DmlBadSyntaxException, DmlNoSuchFieldException}
import org.slf4j.LoggerFactory

/**
  * Created by eqinson on 2017/5/13.
  */
class Range private(override val field: String, val from: Int, val to: Int) extends AbstractPredicate(field, null) with Predicate {
	private val LOGGER = LoggerFactory.getLogger(classOf[Range])

	if (from > to) throw DmlBadSyntaxException("from must be lte to")

	override def eval(tuple: Object): Boolean = {
		val fieldVal = getFiledValFromTupleByName(tuple)
		val fieldType = selector.context.tableMetadata.get(field)
		fieldType match {
			case Some(DataTypes.Int) =>
				fieldVal.asInstanceOf[Integer].compareTo(from) >= 0 && fieldVal.asInstanceOf[Integer].compareTo(to) < 0
			case Some(other)         =>
				LOGGER.error("unsupported data type: {},{}", field, other: Any)
				throw DmlBadSyntaxException("unsupported data type: " + field + "," + other)
			case None                => throw DmlNoSuchFieldException(field)
		}
	}
}

object Range {
	def apply(field: String, from: Int, to: Int) = new Range(field, from, to)
}

