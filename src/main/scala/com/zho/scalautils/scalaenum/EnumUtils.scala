package com.zho.scalautils.scalaenum

object EnumUtils{
object WeekDay extends Enumeration {
  type WeekDay = Value //声明枚举对外暴露的变量类型
  val Mon = Value("1")
  val Tue = Value("2")
  val Wed = Value("3")
  val Thu = Value("4")
  val Fri = Value("5")
  val Sat = Value("6")
  val Sun = Value("7")
  def checkExists(day:String) = this.values.exists(_.toString==day) //检测是否存在此枚举值
  def isWorkingDay(day:WeekDay) = ! ( day==Sat || day == Sun) //判断是否是工作日
  def showAll = this.values.foreach(println) // 打印所有的枚举值
  def cc = Sat
}

  def main(args: Array[String]): Unit = {
    println(WeekDay.Mon.id)
    println(WeekDay.Mon)
    println(WeekDay.cc)

  }
}
class EnumUtils{

}