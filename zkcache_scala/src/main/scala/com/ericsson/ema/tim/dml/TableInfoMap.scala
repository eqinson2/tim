package com.ericsson.ema.tim.dml

/**
  * Created by eqinson on 2017/5/5.
  */
class TableInfoMap {
	private var registry = Map[String, TableInfoContext]()

	def clear(): Unit = {
		registry = Map[String, TableInfoContext]()
	}

	def unregister(tableName: String): Unit = {
		registry -= tableName
	}

	def lookup(tableName: String): Option[TableInfoContext] = registry.get(tableName)

	def registerOrReplace(tablename: String, tableMetadata: Map[String, String], tableData: Object): Unit = {
		registry += (tablename -> TableInfoContext(tableData, tableMetadata))
	}
}

object TableInfoMap {
	var instance: TableInfoMap = new TableInfoMap

	def apply(): TableInfoMap = instance
}

case class TableInfoContext(tabledata: Object, tableMetadata: Map[String, String])
