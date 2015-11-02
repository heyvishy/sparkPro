package com.sp.sparkPro;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sp.sparkPro.domain.Person;

public class JsonDataLoad {

	public static void main(String[] args) {
        System.out.println("load Json data ..." );
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        //objectMapper.configure(Feature.AUTO_CLOSE_SOURCE, true);
        
        SparkConf sc = new SparkConf();
        JavaSparkContext jsc = null;
        
        try{
        	jsc = new JavaSparkContext("local", "jsonDataLoad", sc);
            JavaRDD<String> jsonData = jsc.textFile("src/main/resources/People.json");
            Function<String, Person> mapFunc = new Function<String,Person>(){
    			@Override
    			public Person call(String json) throws Exception {
    				Person person = objectMapper.readValue(json, Person.class);
    				return person;
    			}
    			
            };
            
    		JavaRDD<Person> persons = jsonData.map(mapFunc);
            Function<Person, Boolean> likesFunction =  new  Function<Person, Boolean>(){
    			@Override
    			public Boolean call(Person person) throws Exception {
    				return person.getLikes().contains("cow") || person.getLikes().contains("dog");
    			}
            };

            JavaRDD<Person> animalLoversRDD = persons.filter(likesFunction);
            Function<Person, String> writeJson = new Function<Person,String>(){
    			@Override
    			public String call(Person person) throws Exception {
    				String personString = objectMapper.writeValueAsString(person);
    				return personString;
    			}
            };

            JavaRDD<String> animalLovers =  animalLoversRDD.map(writeJson);
            animalLovers.saveAsTextFile("AnimalLovers");
        }
        catch(Exception e){
        	System.out.println("Exception occurred -->"+e.getMessage());
        }finally{
        	jsc.stop();
        }
	}

}
