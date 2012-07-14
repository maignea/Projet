package com.runmycode.mapred.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.log4j.Logger;
import org.springframework.data.mongodb.core.MongoOperations;

import com.runmycode.mapred.JobConf;
import com.runmycode.mapred.JobID;
import com.runmycode.mapred.OutputCollector;
import com.runmycode.mapred.mongoDB.MongoDBFactory;

public class MapCollector implements OutputCollector<Key, Integer> 
{
	private transient Logger logger = Logger.getLogger(com.runmycode.mapred.impl.MapCollector.class);
	
	//transient MongoOperations mongoOperations = MongoDBFactory.getMongoOperations();
	
	//File folder;
	String path;
	BufferedWriter writer;
	
	//int nbFile = 10;
	
	public MapCollector(String path) throws Exception
	{
		this.path = path;
		//String directory = path.substring(0, path.indexOf("\\mapResult"));
		//File folder = new File(directory);
		String directory = path.substring(0, path.indexOf("/mapResult"));
		URI uri = new URI(directory);
		File folder = new File(uri);
		folder.mkdirs();
		
	}
	
	/*public MapCollector(JobID jobID)
	{
		JobConf job = mongoOperations.findById(jobID.getId(), JobConf.class);
		path = job.getOutputPath();
	}*/


	
	public void open() throws Exception
	{
		
		URI uri = new URI(path+".txt");
		//File file = new File(path+".txt");
		File file = new File(uri);
		writer = new BufferedWriter(new FileWriter(file));
		
		logger.debug("Ouverture fichier : " + file.getPath());
		
	}
	
	@Override
	public void collect(Key key, Integer value) throws IOException
	{
		writer.write("Key="+key.toString()+" Value="+value);
		writer.newLine();
		//writer.flush();
		
		//System.out.println("Ecriture dans le fichier.");
		//writer.close();

	}
	
	public void close() throws IOException
	{
		//nbFile++;
		writer.close();
	}
	
	@Override
	protected void finalize() throws Throwable 
	{
		logger.debug("Méthode finalize()");
		try
		{
			writer.close();
		}
		catch (Exception e) 
		{
			System.out.println("Erreur : writer not close !");
		}
		finally
		{
			super.finalize();
		}
	}
}
