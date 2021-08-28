package com.zho.logger

import org.slf4j.{Logger, LoggerFactory}

class LoggerFactoryUtil {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
}
