package com.ericsson.ema.tim.utils

import java.nio.file.Path

/**
  * Created by eqinson on 2017/5/5.
  */
object FileUtils {
	def readFile(path: Path): String = {
		val source = scala.io.Source.fromFile(path.toUri, "UTF-8")
		val file = source.mkString
		source.close()
		file
	}

}
