package com.ericsson.ema.tim.dml

import java.lang.reflect.{InvocationTargetException, Method}

import com.ericsson.ema.tim.exception.{DmlBadSyntaxException, DmlNoSuchFieldException}
import com.ericsson.ema.tim.zookeeper.ZKPersistenceUtil
import org.json.{JSONArray, JSONObject}
import org.slf4j.LoggerFactory

/**
  * Created by eqinson on 2017/6/23.
  */
abstract class ChangeOperator extends Operator {
	private val LOGGER = LoggerFactory.getLogger(classOf[ChangeOperator])

	protected def cloneList(original: List[Object]): List[Object] = {
		if (original == null || original.isEmpty)
			return List[Object]()

		try {
			val cloneMethod: Method = original.head.getClass.getDeclaredMethod("clone")
			cloneMethod.setAccessible(true)
			for (anOriginal <- original)
				yield cloneMethod.invoke(anOriginal)
		} catch {
			case e@(_: NoSuchMethodException | _: InvocationTargetException | _: IllegalAccessException) =>
				LOGGER.error("Couldn't clone list due to " + e.getMessage)
				List[Object]()
		}
	}

	private[this] def toJson(listOfObj: List[Object]): JSONObject = {
		val json = new JSONObject
		val tableBody = new JSONObject
		json.put("Table", tableBody)
		tableBody.put("Id", this.table)
		val headerArray = new JSONArray
		tableBody.put("Header", headerArray)

		this.context.tableMetadata.foreach(kv => {
			val item = new JSONObject
			item.put(kv._1, kv._2)
			headerArray.put(item)
		})
		val contentArray = new JSONArray
		tableBody.put("Content", contentArray)
		listOfObj.foreach(l => {
			val item = new JSONObject
			item.put("Tuple", l.toString)
			contentArray.put(item)
		})
		json
	}

	protected def realValue(updateVal: (String, String)): Object = {
		val (field, newValue) = updateVal
		this.context.tableMetadata.get(field) match {
			case Some(DataTypes.String)  => newValue
			case Some(DataTypes.Int)     => Integer.valueOf(newValue)
			case Some(DataTypes.Boolean) => java.lang.Boolean.valueOf(newValue)
			case Some(other)             => throw DmlBadSyntaxException("unsupported data type: " + field + "," + other)
			case None                    => throw DmlNoSuchFieldException(field)
		}
	}

	def doExecute(): Unit

	def execute(): Unit = {
		doExecute()
		ZKPersistenceUtil.persist(this.table, toJson(this.records).toString(3))
	}

	def executeDebug(): Unit = {
		doExecute()
		println(toJson(this.records).toString(3))
	}
}
