package xetra.sparksql

import org.apache.spark.sql.SparkSession



/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]) {
    val spark = SparkSession.builder()
      .appName("XETRA SparkSQL")
      .getOrCreate()

    val inputPath = args(0)
    val outputPath = args(1)

    //Create DataFrame from files
    val xetraDF = spark.read.format("csv").option("header","true").option("inferSchema","true").load(inputPath)

    // Register as Temp View
    xetraDF.createOrReplaceTempView("xetra")

    // Find Biggest Winner Statement
    val xetraBiggestWinnerSQL =
      """
        |select allreturns.Date, allreturns.SecurityID, allreturns.SecurityDesc, allreturns.percentchange
        |
        |from (
        |
        |    select xetra1.Date, max((xetra2.EndPrice - xetra1.StartPrice) / xetra1.StartPrice) as percentchange
        |
        |    from (
        |
        |        select SecurityId, min(Time) as mintime, max(Time) as maxtime, Date from xetra
        |        group by SecurityId, Date
        |    ) as timestamps
        |
        |    left join xetra as xetra1
        |    on xetra1.SecurityId = timestamps.SecurityId and xetra1.Date = timestamps.Date and xetra1.Time = timestamps.mintime
        |
        |    left join xetra as xetra2
        |    on xetra2.SecurityId = timestamps.SecurityId and xetra2.Date = timestamps.Date and xetra2.Time = timestamps.maxtime
        |
        |    group by xetra1.Date
        |) as maxreturns
        |
        |left join (
        |    select xetra1.SecurityId, xetra1.SecurityDesc, xetra1.Date, (xetra2.EndPrice - xetra1.StartPrice) / xetra1.StartPrice as percentchange
        |
        |    from (
        |
        |        select SecurityId, min(Time) as mintime, max(Time) as maxtime, Date from xetra
        |        group by SecurityId, Date
        |    ) as timestamps
        |
        |    left join xetra as xetra1
        |    on xetra1.SecurityId = timestamps.SecurityId and xetra1.Date = timestamps.Date and xetra1.Time = timestamps.mintime
        |
        |    left join xetra as xetra2
        |    on xetra2.SecurityId = timestamps.SecurityId and xetra2.Date = timestamps.Date and xetra2.Time = timestamps.maxtime
        |) as allreturns
        |
        |on maxreturns.percentchange = allreturns.percentchange and maxreturns.Date = allreturns.Date
        |where maxreturns.percentchange > 0
        |order by allreturns.Date
      """.stripMargin

    // Query Biggest Winners
    val xetraBiggestWinnersDF = spark.sql(xetraBiggestWinnerSQL)
    println("XETRA BIGGEST WINNERS")
    xetraBiggestWinnersDF.show()

    // Write result to file
    xetraBiggestWinnersDF.coalesce(1).write.option("header","true").csv(outputPath + "/xetraBiggestWinners")

    // Find Highest Volume Statement
    val xetraHighestVolumeSQL =
      """
        |select SecurityDesc, SecurityID, avg((MaxPrice - MinPrice) / MinPrice) as implied_vol from xetra
        |group by SecurityDesc, SecurityID
        |order by implied_vol desc
      """.stripMargin

    // Query Highest Volume
    val xetraHighestVolumeDF = spark.sql(xetraHighestVolumeSQL)
    println("XETRA HIGHEST VOLUME")
    xetraHighestVolumeDF.show()

    // Write Highest Volume to file
    xetraHighestVolumeDF.coalesce(1).write.option("header","true").csv(outputPath + "/xetraHighestVolume")

    // Find Most Traded By Volume Statement
    val xetraMostTradedByVolumeSQL =
      """
        |select sumtable.Date, sumtable.SecurityId, sumtable.SecurityDesc, maxtable.maxamount
        |
        |from (
        |    select date, max(tradeamountmm) as maxamount
        |
        |    from (
        |
        |        select SecurityId, SecurityDesc, Date, sum(StartPrice * TradedVolume) / 1e6 as tradeamountmm
        |        from xetra
        |        where Currency = 'EUR'
        |        group by SecurityId, SecurityDesc, Date
        |        order by Date asc, tradeamountmm desc
        |    )
        |
        |    group by Date
        |) as maxtable
        |
        |left join (
        |    select SecurityId, SecurityDesc, Date, sum(StartPrice * TradedVolume) / 1e6 as tradeamountmm
        |    from xetra
        |    where Currency = 'EUR'
        |    group by SecurityId, SecurityDesc, Date
        |    order by Date asc, tradeamountmm desc
        |) as sumtable
        |
        |on sumtable.tradeamountmm = maxtable.maxamount and sumtable.Date = maxtable.Date
        |where maxtable.maxamount > 0
        |order by sumtable.Date asc, maxtable.maxamount desc
      """.stripMargin

    // Query Most Traded By Volume
    val xetraMostTradedByVolumeDF = spark.sql(xetraMostTradedByVolumeSQL)
    println("XETRA MOST TRADED BY VOLUME")
    xetraMostTradedByVolumeDF.show()

    // Write result to file
    xetraMostTradedByVolumeDF.coalesce(1).write.option("header","true").csv(outputPath + "/xetraMostTradedByVolume")

  }

}
