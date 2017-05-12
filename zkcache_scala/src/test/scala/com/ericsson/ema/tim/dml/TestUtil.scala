package com.ericsson.ema.tim.dml

import java.io.FileNotFoundException
import java.nio.file.Paths

import com.ericsson.ema.tim.utils.FileUtils
import com.ericsson.ema.tim.zookeeper.ZKMonitor

/**
  * Created by eqinson on 2017/5/12.
  */
object TestUtil {
	def init(testFile: String, tableName: String): Unit = {
		val url = Thread.currentThread.getContextClassLoader.getResource(testFile)
		if (url != null) {
			val zm = new ZKMonitor(null)
			zm.doLoad(tableName, FileUtils.readFile(Paths.get(url.toURI)))
		}
		else throw new FileNotFoundException(testFile + " not found")
	}

	def printResult(sliceRes: List[List[Object]]): Unit = {
		for (eachRow <- sliceRes) {
			if (eachRow.isInstanceOf[List[_]]) {
				val row = eachRow.asInstanceOf[List[Object]]
				row.foreach((r: Object) => print(r + "   "))
			}
			println
		}
	}

	def printResultGroup(mapRes: Map[Object, List[Object]]): Unit = {
		mapRes.foreach(kv => {
			if (kv._2 != null) {
				val row = kv._2.asInstanceOf[List[Object]]
				row.foreach((r: Object) => print(r + "   "))
			}
			println
		})
	}
}
