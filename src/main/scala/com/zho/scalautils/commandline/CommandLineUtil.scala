package com.zho.scalautils.commandline

import com.zho.logger.LoggerFactoryUtil
import org.apache.commons.cli._

/**
 * 命令行是 参数解析工具类：
 * addRequiredOption 此方式的参数 必须存在 不存在程序回抛出异常
 * addOption 可选参数
 * addOption  false  传入参数为 --help    true 传入参数为 --source_db_type mysql
 */
class CommandLineUtil extends LoggerFactoryUtil {

  def parameterAnalysis(args: Array[String]): CommandLine = {
    val commandOptions = new Options()
    commandOptions.addOption("h", "help", false, "Lists short help")
    commandOptions.addRequiredOption("D", "source_db_type", true, "source database type, oracle or mysql or mssql")
    //    commandOptions.addRequiredOption("U", "url", true, "source database url")
    //    commandOptions.addRequiredOption("u", "user", true, "source database user")
    //    commandOptions.addRequiredOption("p", "password", true, "source database password")
    //    commandOptions.addRequiredOption("t", "source_table", true, "source table")
    //    commandOptions.addRequiredOption("T", "target_table", true, "target table")
    //    commandOptions.addRequiredOption("S", "target_store_type", true, "target store type, kudu or parquet")
    commandOptions.addOption("r", "drop_and_recreate_table", false, "drop table and rebuild target table")
    commandOptions.addOption("c", "split_by", true, "Concurrence Read Data use split_by, oracle default rowid")
    commandOptions.addOption("n", "split_num", true, "Concurrence Read Data use split_num, default 1, range 1 to 100")
    commandOptions.addOption("k", "kudu_primary_key", true, "The Kudu table's primary key,default origin table primary key, split by comma")
    commandOptions.addOption("w", "where_clause", true, "Tables where clause")
    commandOptions.addOption("a", "allow_text_splitter", false, "regard split_by column as string to split")
    commandOptions.addOption("m", "sync_mode", true, "sync mode is full or increment")
    commandOptions.addOption("o", "hive_overwrite", false, "hive overwrite")
    commandOptions.addOption("d", "enable_delete_upsert", false, "delete and upsert")
    commandOptions.addOption("", "ignore_source_schema_change", false, "ignore schema change(source add column)")
    commandOptions.addOption("", "mcc_table_id", true, "mcc_table_id")
    //    commandOptions.addRequiredOption("", "instanceid", true, "instanceid")
    commandOptions.addOption("", "kudu_paritition_num", true, "kudu_paritition_num")
    val parser: CommandLineParser = new DefaultParser()
    var cmd: CommandLine = null
    try {
      cmd = parser.parse(commandOptions, args)
    } catch {
      case _: Exception =>
        val hf: HelpFormatter = new HelpFormatter()
        hf.setWidth(110)
        this.logger.error("This input parameter don't satisfy this program, please see the help\n",
          hf.printHelp("OracleTest", commandOptions, true))
        System.exit(0)
    }
    cmd
  }
}

/**
 * Update Date:2021-08-28 15:00
 * Anthor:zhoulei
 */
object CommandLineUtil extends LoggerFactoryUtil {

  def main(args: Array[String]): Unit = {

    val arg = Array[String]("--source_db_type","oracle")

    println(new CommandLineUtil().parameterAnalysis(arg).getOptionValue("source_db_type"))


  }

}