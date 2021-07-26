package com.zho.scalautils.analyidcard

import java.net.URL
import java.util.Calendar
import scala.io.{BufferedSource, Source}


/**
 * @author zholei
 *         身份证号解析demo
 *
 *         根据《中华人民共和国国家标准GB 11643-1999》中有关公民身份号码的规定，
 *         公民身份号码是特征组合码，由十七位数字本体码和一位数字校验码组成。排列顺序从左至右依次为：
 *         六位数字地址码，八位数字出生日期码，三位数字顺序码和一位数字校验码。
 *         顺序码的奇数分给男性，偶数分给女性。校验码是根据前面十七位数字码，
 *         按照ISO 7064:1983.MOD 11-2校验码计算出来的检验码。
 *
 *         最后一位：18位数据计算规则：
 *         居民身份证是国家法定的证明公民个人身份的有效证件．身份证号码由十七位数字本体码和一位数字校验码组成．
 *         第1-6位是地址码，第7-14位是出生日期码，第15-17位是顺序码，即是县、区级政府所辖派出所的分配码．
 *         第18位也就是最后一位是数字校验码，是根据前面十七位数字码，按一定规则计算出来的校验码．算法如下：
 *         规定第1-17位对应的系数分别为：7，9，10，5，8，4，2，1，6，3，7，9，10，5，8，4，2．
 *         将身份证号码的前17位数字分别乘以对应的系数，再把积相加．相加的结果除以11，求出余数．
 *         余数只可能有0，1，2，3，4，5，6，7，8，9，10这11种情况．其分别对应身份证号码的第18位数字如表所示．
 *         余  数	0	1	2	3	4	5	6	7	8	9	10
 *         第18位	1	0	x	9	8	7	6	5	4	3	2
 *         通过上面得知如果余数是3，则身份证的第18位数字就是9．如果余数是2，则身份证的第18位号码就是x．若
 *         某人的身份证号码的前17位依次是11010219600302011，则他身份证号码的第18位数字是3．
 *         date 2021-07-21
 */
object AnalysisIDCard {
  def main(args: Array[String]): Unit = {

    // 身份证 15位、18位 正则匹配
    // ^[1-9]\d{7}((0\d)|(1[0-2]))(([0|1|2]\d)|3[0-1])\d{3}$|^[1-9]\d{5}[1-9]\d{3}((0\d)|(1[0-2]))(([0|1|2]\d)|3[0-1])\d{3}([0-9]|X)$
    val idCard = "11022418750909281X"
    //    val regex = "^[1-9]\\d{7}((0\\d)|(1[0-2]))(([0|1|2]\\d)|3[0-1])\\d{3}$|^[1-9]\\d{5}[1-9]\\d{3}((0\\d)|(1[0-2]))(([0|1|2]\\d)|3[0-1])\\d{3}([0-9]|X)$".r
    //    if (regex.findPrefixOf(idCard) != None) {
    //      println(regex.findPrefixOf(idCard).get)
    //    } else {
    //      println("无效匹配")
    //    }


    if (idCard.length == 18) {
      println("************************* 查询结果 *************************")
      println(s"*　　 您查询的身份证号码 　　: $idCard")
      println(s"*　　 性别　　　　　　　 　　: ${getSexByIdCard(idCard)}")
      println(s"*　　 出生日期　　　　　 　　: ${getYearByIdCard(idCard)} 年 ${getMontyByIdCard(idCard)} 月 ${getDayByIdCard(idCard)}")
      println(s"*　　 原籍地　　　　　　 　　: ${getAddressByIdCard(idCard)} ${
        if (idCard.substring(17, 18).equals(getCheckCode(idCard))) {
          ""
        } else {
          "(提示：该18位身份证号校验位不正确)"
        }
      } ")
      println(s"*    身份证效验码　　　　　 : ${getCheckCode(idCard)}")
      println("************************* 查询结果 *************************")
    } else {
      println("ID Card Is Error")
      // Result
      // ************************* 查询结果 *************************
      // *　　 您查询的身份证号码 　　: 11010219600302011X
      // *　　 性别　　　　　　　 　　: 男
      // *　　 出生日期　　　　　 　　: 1960 年 3 月 2
      // *　　 原籍地　　　　　　 　　: 北京市西城区
      // ************************* 查询结果 *************************
    }

  }

  /**
   * 根据身份编号获取生日年
   *
   * @param idCard idCard 身份证号码
   * @return 生日(yyyy)
   */
  def getYearByIdCard(idCard: String): Int = {
    idCard.substring(6, 10).toInt
  }

  /**
   * 根据身份编号获取生日月
   *
   * @param idCard idCard 身份证号码
   * @return 生日(MM)
   */
  def getMontyByIdCard(idCard: String): Int = {
    idCard.substring(10, 12).toInt
  }

  /**
   * 根据身份编号获取生日天
   *
   * @param idCard idCard 身份证号码
   * @return 生日(dd)
   */
  def getDayByIdCard(idCard: String): Int = {
    idCard.substring(12, 14).toInt
  }

  /**
   *
   * @param idCard 身份证号码
   * @return 年龄
   */
  def getAgeByIdCard(idCard: String): Int = {
    val cal = Calendar.getInstance()
    val idCardYear: Int = idCard.substring(6, 10).toInt
    val currentYear: Int = cal.get(Calendar.YEAR)
    currentYear - idCardYear
  }

  /**
   *
   * @param idCard idCard 身份证号码
   * @return 性别
   */
  def getSexByIdCard(idCard: String): String = {
    var sGender = "未知"
    val sexFlag = idCard.substring(16, 17).toInt
    if (sexFlag % 2 != 0) {
      sGender = "1"
    } else {
      sGender = "2"
    }
    if (sGender.equals("1")) "男" else "女"
  }

  /**
   *
   * @param idCard idCard 身份证号码
   * @return 原籍地
   */
  def getAddressByIdCard(idCard: String): String = {
    val addressCode = idCard.substring(0, 6)
    // 在 resources 目录中 获取数据文件路径
    val dataUrl: URL = this.getClass.getClassLoader.getResource("IdCardAreaCode.txt")
    // 根据路径加载 文件数据
    val data: BufferedSource = Source.fromURL(dataUrl)
    // 获取文件数据里的 所有行 返回数据集合
    val linData: Iterator[String] = data.getLines()
    val resultData: Iterator[(String, String)] = linData.map(elem => {
      val codeAreaList: Array[String] = elem.split(" ")
      (codeAreaList(0), codeAreaList(1))
    })
    resultData.filter(_._1.equals(addressCode)).map(_._2).toList.head
  }

  /**
   *
   * @param idCard 身份证号码
   * @return 身份证效验码
   */
  def getCheckCode(idCard: String): String = {
    val idCardElemList = idCard.toList
    val checkCode: Int = idCardElemList.head * 7 + idCardElemList(1) * 9 + idCardElemList(2) * 10 + idCardElemList(3) * 5 + idCardElemList(4) * 8 + idCardElemList(5) * 4 + idCardElemList(6) * 2 + idCardElemList(7) * 1 + idCardElemList(8) * 6 + idCardElemList(9) * 3 + idCardElemList(10) * 7 + idCardElemList(11) * 9 + idCardElemList(12) * 10 + idCardElemList(13) * 5 + idCardElemList(14) * 8 + idCardElemList(15) * 4 + idCardElemList(16) * 2

    checkCode % 11 match {
      case 0 => "1"
      case 1 => "0"
      case 2 => "X"
      case 3 => "9"
      case 4 => "8"
      case 5 => "7"
      case 6 => "6"
      case 7 => "5"
      case 8 => "4"
      case 9 => "3"
      case 10 => "2"
      case _ => null
    }

  }
}
