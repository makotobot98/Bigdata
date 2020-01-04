package com.makoto.spark.conf
import org.rogach.scallop._

class Configuration(arguments: Seq[String]) extends ScallopConf(arguments) {
  val userListPath: ScallopOption[String] = opt[String](
    "user.list.path",
    descr = "path to the user list",
    required = false,
    default = Option("./unique_user_list")
  )
  //arg for the path to the config file specifying DataFrame fields to select
  // in key-value pair
  val selectColumnsConfigFile: ScallopOption[String] = opt[String](
    "select.columns.config",
    descr = "name of columns in the dataframe to select",
    required = false,
    default = Option("SelectDF")
  )
  val inputMetaDataPath: ScallopOption[String] =
    opt[String]("input.meta.path",
      descr = "input meta data parent path",
      required = false,
      default = env() match {
        case "test" => Option("gs://path")
        case "stage" => Option("gs://path")
        case "prod" => Option("gs://path")
        case _ => None
          throw new Exception(s"env error, env name can either be test, stage, prod \ncannot be ${env()}")
      })
  val outputBikeTripPath: ScallopOption[String] =
    opt[String]("output.data.path",
      descr = "output data parent path",
      required = false,
      default = env() match {
        case "test" => Option("gs://jiuzhangsuanfa/output")
        case "stage" => Option("gs://jiuzhangsuanfa/output")
        case "prod" => Option("gs://jiuzhangsuanfa/output")
        case _ => None
          throw new Exception(s"env error, env name can either be test, stage, prod \ncannot be ${env()}")
      })
  val inputBikeSharePath: ScallopOption[String] =
    opt[String]("input.bike.path",
      descr = "input data path for bike share data",
      required = false,
      default = env() match {
        case "test" => Option("gs://testpath")
        case "stage" => Option("./")
        case "prod" => Option("gs://prodpath")
        case _ => None
          throw new Exception(s"env error, env name can either be test, stage, prod \ncannot be ${env()}")
      }
    )

  val env: ScallopOption[String] = opt[String]("env",
    descr = "env name that job is running on, test, stage, prod",
    required = false,
    default = Option("stage"))

  val datePrefix: ScallopOption[String] =
    opt[String]("date.prefix",
      descr = "date prefix for path",
      required = false,
      default = Option("start_date"))

  val processDate: ScallopOption[String] =
    opt[String]("process.date",
      descr = "date to process, in YYYY-MM-DD format",
      required = true)
  //specifying how many day ago bike trip data needed, {1, 3, 7}
  val dayAgo: ScallopOption[Int] =
    opt[Int]("day.ago",
      descr = "how many day ago data required, must be 1, 3, or 7",
      required = false,
      default = Option(1))
  verify()
}
