package com.ericsson.ema.tim.json


import org.json.{JSONArray, JSONObject}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by eqinson on 2017/5/5.
  */
case class FieldInfo(fieldValue: String, fieldName: String, fieldType: String) {
	override def toString: String = "FieldInfo{" + "fieldValue='" + fieldValue + '\'' + ", fieldName='" + fieldName + '\'' + ", fieldType='" + fieldType + '\'' + '}'
}

case class TypeInfo(theName: String, theType: String) {
	override def toString: String = "TypeInfo{" + "name='" + theName + '\'' + ", type='" + theType + '\'' + '}'
}


class JsonLoader(var tableName: String) {
	private val LOGGER = LoggerFactory.getLogger(classOf[JsonLoader])

	private val TABLE_TAG = "Table"
	private val ID_TAG = "Id"
	private val TABLE_HEADER_TAG = "Header"
	private val TABLE_CONTENT_TAG = "Content"
	private val TABLE_TUPLE_TAG = "Tuple"
	private val PATTERN = "\\{[\\w ]+\\}".r
	private val tableHeaderIndexMap: mutable.Map[Integer, TypeInfo] = mutable.Map[Integer, TypeInfo]()

	val tableMetadata: mutable.Map[String, String] = mutable.LinkedHashMap[String, String]()
	var tupleList: List[List[FieldInfo]] = List[List[FieldInfo]]()

	private def trimBrace(s: String): String = {
		if (s.length >= 2) s.substring(1, s.length - 1)
		else ""
	}

	private def parseTableHeader(root: JSONObject): Unit = {
		val arr: JSONArray = root.getJSONArray(TABLE_HEADER_TAG)
		var i: Int = 0
		for (i <- 0 until arr.length) {
			val keys = arr.getJSONObject(i).keys
			while (keys.hasNext) {
				val key: String = keys.next
				arr.getJSONObject(i).get(key) match {
					case t: String => tableHeaderIndexMap.put(i, TypeInfo(key, t)); tableMetadata.put(key, t)
					case _         => throw new ClassCastException("bug: illegal type...")
				}
			}
		}
	}

	private def parseTableContent(root: JSONObject): Unit = {
		val arr: JSONArray = root.getJSONArray(TABLE_CONTENT_TAG)
		var i: Int = 0
		for (i <- 0 until arr.length) {
			val content = arr.getJSONObject(i).getString(TABLE_TUPLE_TAG)
			var tuple = List[FieldInfo]()
			var column = 0
			for (matchedField <- PATTERN.findAllIn(content)) {
				tableHeaderIndexMap.get(column) match {
					case Some(f) => tuple :+= FieldInfo(trimBrace(matchedField), f.theName, f.theType)
					case None    => throw new RuntimeException("bug: illegal tableHeaderIndexMap content...")
				}
				column += 1
			}
			tupleList :+= tuple
		}
	}

	def loadJsonFromString(jsonStr: String): Unit = {
		val obj = new JSONObject(jsonStr)
		val table = obj.getJSONObject(TABLE_TAG)
		if (tableName == null)
			tableName = table.getString(ID_TAG)

		parseTableHeader(table)
		if (LOGGER.isDebugEnabled) {
			tableHeaderIndexMap.foreach(kv => LOGGER.debug("key : {}, value: {}", kv._1, kv._2: Any))
			tableMetadata.foreach(kv => LOGGER.debug("key : {}, value: {}", kv._1, kv._2: Any))
		}

		parseTableContent(table)
		if (LOGGER.isDebugEnabled) {
			tupleList.foreach(_.foreach(LOGGER.debug("field info: {}", _)))
		}
	}
}
