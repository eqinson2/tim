package com.ericsson.ema.tim.dml.group

import com.ericsson.ema.tim.dml.{DataTypes, SelectClause}
import com.ericsson.ema.tim.exception.{DmlBadSyntaxException, DmlNoSuchFieldException}
import org.slf4j.LoggerFactory

/**
  * Created by eqinson on 2017/5/13.
  */
class GroupBy(private val field: String) extends SelectClause {
	private val LOGGER = LoggerFactory.getLogger(classOf[GroupBy])

	override def getField: String = field

	type keyExtractorFuncType = (Object => Object)

	def keyExtractor(): keyExtractorFuncType = {
		selector.context.tableMetadata.get(field) match {
			case Some(DataTypes.String) | Some(DataTypes.Int) | Some(DataTypes.Boolean) =>
				o: Object => getFiledValFromTupleByName(o)
			case Some(otherType)                                                        =>
				LOGGER.error("unsupported data type: {},{}", field, otherType: Any)
				throw DmlBadSyntaxException("Error: unsupported data type: " + field + "," + otherType)
			case None                                                                   =>
				throw DmlNoSuchFieldException(field)

		}
	}
}


