package com.runmycode.mapred.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.log4j.Logger;
import org.springframework.data.mongodb.core.MongoOperations;

import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Paragraph;
import com.itextpdf.text.log.SysoLogger;
import com.itextpdf.text.pdf.PdfWriter;
import com.runmycode.mapred.JobConf;
import com.runmycode.mapred.JobID;
import com.runmycode.mapred.OutputCollector;
import com.runmycode.mapred.mongoDB.MongoDBFactory;

public class ReducerCollector implements OutputCollector<String, String> 
{
	private transient Logger logger = Logger.getLogger(com.runmycode.mapred.impl.ReducerCollector.class);
	//private transient MongoOperations mongoOperations = MongoDBFactory.getMongoOperations();
	
	//private File folder;
	private String path;
	private int i;
	
	private Document document;
	private PdfWriter writer;
	
	private JobConf job;

	//private int nbFile = 10;
	//int nbFile = 10;
	
	public ReducerCollector(String path) throws Exception
	{
		this.path = path;
		String directory = path.substring(0, path.indexOf("/reduceResult"));
		File folder = new File(new URI(directory));
		folder.mkdirs();
		
	}
	
	
	/*public ReducerCollector(JobID jobID)
	{
		job = mongoOperations.findById(jobID.getId(), JobConf.class);
		path = job.getOutputPath();
	}*/

	public void open() throws Exception
	{
		i = 0;

		File file = new File(new URI(path+".pdf"));
		
		/*if(folder == null)
		{
			path += "\\ReduceResults";
			folder = new File(path);
			if (folder.mkdirs()) 
			{		
				logger.debug("Ajout des dossiers : " + folder.getPath());			
			}
		}
			
		File file = new File(path+"\\reduce_"+nbFile+".pdf");*/
		
		try 
		{
			document = new Document();
			
			writer = PdfWriter.getInstance(document, new FileOutputStream(file));
			
			document.open();	
			
			document.add(new Paragraph("Résultat du job avec l'opération n + n pour n = {0:1:10}"));
		} 
		catch (DocumentException e) 
		{
			e.printStackTrace();
		}			
	}
	@Override
	public void collect(String key, String resultValue) throws IOException 
	{		
		try
		{	
			document.add(new Paragraph("Avec n="+i+" : "+resultValue));
	
			i++;	        	
		} 
		catch (DocumentException e) 
		{
			e.printStackTrace();
		}
	}
	
	public void close()
	{
		logger.debug("Close ReducerCollector");
		//nbFile++;
		document.close();
		writer.close();
	}
}
