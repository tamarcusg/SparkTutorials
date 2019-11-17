package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

	public static void main(String[] args) {
		
		List<Double> inputData = new ArrayList<>();
		
		inputData.add(34.4);
		inputData.add(4.24);
		inputData.add(987.56);
		inputData.add(324.234);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		//gets Spark up and running
		SparkConf conf = new SparkConf().setAppName("learningSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
  		JavaRDD<Double> myRdd = sc.parallelize(inputData);
		
		Double result = myRdd.reduce( (value1,value2) -> value1 + value2);
		
		System.out.println(result);
		
		sc.close();
	}

}
