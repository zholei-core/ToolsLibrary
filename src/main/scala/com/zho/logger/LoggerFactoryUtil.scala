package com.zho.logger

import org.slf4j.{Logger, LoggerFactory}

trait LoggerFactoryUtil {

  // 日志公调用类
  def logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName.stripSuffix("$"))

}
