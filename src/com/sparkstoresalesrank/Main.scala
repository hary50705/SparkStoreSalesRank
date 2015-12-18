package com.sparkstoresalesrank

import breeze.linalg.split
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * Created by User on 2015/12/17.
 */
object Main {
  def main(args: Array[String]) {
    val inputFile = args(0)//輸入檔案路徑
    val outputFile = args(1)//輸出檔案路徑

    //val conf=new SparkConf().setAppName("sparkstoresalesrank").setMaster("local")//Spark配置
    val conf=new SparkConf().setAppName("sparkstoresalesrank")//Spark配置
    val sc=new SparkContext(conf)//Spark功能接口

    val salesdata=sc.textFile(args(0))//讀取輸入文字檔內容
    val salesdataFlat=salesdata.flatMap(line => {
        /*取得plist資料*/
        val orderdata = line.split(";")(3)
        val plistItem = orderdata.substring(orderdata.indexOf("=") + 1)
        val data = plistItem.split(",")//plist各個資料項

        if (data.size > 1) {
          var plistItemArray = new Array[String](data.size / 3)
          var num = 0
          for (i <- 0 to data.size-1 by 3) {
            plistItemArray(num) = data(i) + ',' + data(i+1) + ',' + data(i+2)
            num += 1
          }
          plistItemArray
        }else{
          null
        }
      })

    val salesdataMap=salesdataFlat.map(item =>{
        val itemdata=item.split(",")//itemdata(0)產品識別碼  itemdata(1)產品數量 itemdata(2)產品售價
        (itemdata(0),(itemdata(1).toInt * itemdata(2).toInt))
    })

    val salesdataReduce=salesdataMap.reduceByKey((x,y) => x+y)//將Key一樣的進行統計

    var rankArray=salesdataReduce.sortBy(pair =>pair._2,false).take(20)//以Value由大到小進行排序且取20筆

    var resultArray=new Array[(Int,String)](rankArray.length)
    for(i <- 0 to rankArray.length-1){
      resultArray(i)=(i+1,rankArray(i)_1)
    }

    sc.parallelize(resultArray).saveAsTextFile(outputFile)//儲存
  }
}
