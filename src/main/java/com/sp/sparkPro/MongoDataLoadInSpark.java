package com.sp.sparkPro;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.bson.BSONObject;

import scala.Tuple2;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.hadoop.MongoOutputFormat;

public class MongoDataLoadInSpark {

	public static void main(String[] args) {
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);

        SparkConf sc = new SparkConf();
        JavaSparkContext jsc = null;
        try{
            jsc = new JavaSparkContext("local", "MongoDataLoadInSpark");
            Configuration conf = new Configuration();
            conf.set("mongo.input.uri", "mongodb://127.0.0.1:27017/SparkDemo.people");
            conf.set("mongo.output.uri", "mongodb://127.0.0.1:27017/SparkDemo.output");
            
            final JavaPairRDD<Object, BSONObject> mongoRDD = jsc.newAPIHadoopRDD(conf, com.mongodb.hadoop.MongoInputFormat.class, Object.class, BSONObject.class);
            System.out.println("Loaded all people data from mongoDB 'people' Collection...Now going to find people who are animal lovers in this data set");
            Function<Tuple2<Object, BSONObject>, Boolean> filterFunc = new Function<Tuple2<Object, BSONObject>, Boolean>(){
				@Override
				public Boolean call(Tuple2<Object, BSONObject> v1) throws Exception {
    				String likes = (String)v1._2().get("likes");
    				return likes.contains("cow") || likes.contains("dog");
				}
            };
            
            JavaPairRDD<Object, BSONObject> filteredMongoDocs = mongoRDD.filter(filterFunc);
            
            System.out.println("Filtered animal lover people and Now writing it in 'output' collection in MongoDB");
            // Only MongoOutputFormat and conf are relevant
            filteredMongoDocs.saveAsNewAPIHadoopFile("file:///bogus", Object.class, Object.class, MongoOutputFormat.class, conf);
            
        }catch(Exception e){
        	System.out.println("Exception occurred "+e.getMessage());
        }finally{
        	jsc.stop();
        }
	} 
}
