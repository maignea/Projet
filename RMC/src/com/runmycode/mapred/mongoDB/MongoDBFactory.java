package com.runmycode.mapred.mongoDB;

import java.net.UnknownHostException;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;

import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class MongoDBFactory 
{
	
	static MongoOperations mongoOperations;
	
	static{
		try {
			mongoOperations = new MongoTemplate(new SimpleMongoDbFactory(new Mongo(), "database"));
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MongoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
		
	static public MongoOperations getMongoOperations()
	{	
		//ApplicationContext ctx = new GenericXmlApplicationContext("applicationContextMongoDB.xml");		    
		//mongoOperations = (MongoOperations)ctx.getBean("mongoTemplate");
			
		return mongoOperations;
	}
}
