package com.sp.sparkPro;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Log Analysis
 *
 */
public class LogAnalysis 
{
    public static void main( String[] args )
    {
        System.out.println("Program to Analyze bad lines in log file" );
        SparkConf sc = new SparkConf();
        JavaSparkContext jsc = new JavaSparkContext("local", "badLineAnalyzer", sc);
        
        JavaRDD<String> logLines = jsc.textFile("C:\\Users\\vshukla\\workspace\\sparkPro\\src\\main\\resources\\poi.log");
        Function<String, Boolean> badLinesFunction =  new  Function<String, Boolean>(){
			@Override
			public Boolean call(String line) throws Exception {
				return line.contains("Error") || line.contains("error") || line.contains("exception") || line.contains("Exception");
			}
        };
        
        JavaRDD<String> badLines = logLines.filter(badLinesFunction);
        badLines.saveAsTextFile("BadLines");
        
        
        
    }
}
