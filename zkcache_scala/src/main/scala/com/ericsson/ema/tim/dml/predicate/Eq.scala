package com.ericsson.ema.tim.dml.predicate


import com.ericsson.ema.tim.dml.DataTypes
import com.ericsson.ema.tim.exception.{DmlBadSyntaxException, DmlNoSuchFieldException}
import org.slf4j.LoggerFactory

/**
  * Created by eqinson on 2017/5/12.
  */
class Eq private(override val field: String, override val valueToComp: Object) extends AbstractPredicate(field, valueToComp) with Predicate {
	private val LOGGER = LoggerFactory.getLogger(classOf[Eq])

	override def eval(tuple: Object): Boolean = {
		if (this.valueToComp eq null)
			return true

		val fieldVal: Object = getFiledValFromTupleByName(tuple)
		val fieldType = selector.context.tableMetadata.get(field)
		fieldType match {
			case Some(DataTypes.String)  => this.valueToComp == fieldVal
			case Some(DataTypes.Int)     => Integer.valueOf(this.valueToComp.asInstanceOf[String]) == fieldVal.asInstanceOf[Integer]
			case Some(DataTypes.Boolean) => java.lang.Boolean.valueOf(this.valueToComp.asInstanceOf[String]) == fieldVal.asInstanceOf[java.lang.Boolean]
			case Some(_)                 =>
				LOGGER.error("unsupported data type: {},{}", field, fieldType: Any)
				throw DmlBadSyntaxException("unsupported data type: " + field + "," + fieldType)
			case None                    => throw DmlNoSuchFieldException(field)
		}
	}
}

object Eq {
	def apply(field: String, value: String) = new Eq(field, value)
}
