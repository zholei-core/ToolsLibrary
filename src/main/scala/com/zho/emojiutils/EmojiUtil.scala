package com.zho.emojiutils

import com.vdurmont.emoji.EmojiParser

object EmojiUtil {
  def main(args: Array[String]): Unit = {

    val str: String = "Here is a boy: :boy|type_6:!"
    println(s"原始字符为：$str")

    /*
    1、将表情转换成对应别名字符 （to aliases）

    转换成字符别名，使用以下两种方法即可。
    EmojiParser.parseToAliases(str); 直接转换
    EmojiParser.parseToAliases(str, FitzpatrickAction); 带修改器的转换，修改器可以指定不同的样式
     */

    println("to aliases 之后：")
    println(EmojiParser.parseToAliases(str))
    println(EmojiParser.parseToAliases(str, EmojiParser.FitzpatrickAction.PARSE))
    println(EmojiParser.parseToAliases(str, EmojiParser.FitzpatrickAction.REMOVE))
    println(EmojiParser.parseToAliases(str, EmojiParser.FitzpatrickAction.IGNORE))

    /*
    2、将表情转换成html（to html）

    转换成html，可以使用如下，后者为十六进制的表现方式。
    EmojiParser.parseToHtmlDecimal(str)； 直接转换
    EmojiParser.parseToHtmlHexadecimal(str); 直接转换（十六进制）
    EmojiParser.parseToHtmlDecimal(str, FitzpatrickAction); 带修改器的转换
     */
    println("to html：")
    println(EmojiParser.parseToHtmlDecimal(str))
    println(EmojiParser.parseToHtmlDecimal(str, EmojiParser.FitzpatrickAction.PARSE))
    println(EmojiParser.parseToHtmlDecimal(str, EmojiParser.FitzpatrickAction.REMOVE))
    println(EmojiParser.parseToHtmlDecimal(str, EmojiParser.FitzpatrickAction.IGNORE))

    println("to html(hex)：")
    println(EmojiParser.parseToHtmlHexadecimal(str))

    /*
    3、再次转换回表情
    经过转换之后的字符就可以存到数据库了， 那么从数据库中取出来后，将字符还原成emoji表情使用如下方法即可。
    EmojiParser.parseToUnicode(str);
     */

    println("to html：")
    val strHtml = EmojiParser.parseToHtmlDecimal(str)
    println(strHtml)

    println("还原：")
    println(EmojiParser.parseToUnicode(strHtml))
  }
}

