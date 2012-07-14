package com.runmycode.mapred.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.QueueConnection;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.oxm.Marshaller;
import org.springframework.oxm.Unmarshaller;
import org.springframework.oxm.castor.CastorMarshaller;

import com.runmycode.mapred.DataIndex;
import com.runmycode.mapred.JobID;
import com.runmycode.mapred.Key;
import com.runmycode.mapred.Reporter;
import com.runmycode.mapred.TaskStatus;
import com.runmycode.mapred.server.ComputingReportationMessage;
import com.runmycode.ro.InstanceSet;
import com.runmycode.ro.Param;

public class SplitterThread extends Thread{
	
	Logger logger = Logger.getLogger(com.runmycode.mapred.impl.SplitterThread.class);
	
	private InstanceSet instanceSet;
	private String configPath;
	private String indexPath;
	private Reporter reporter;
	private QueueSession computingReportationQueueSession;
	private QueueSender computingReportationQueueSender;

	private Marshaller marshaller;
	private Unmarshaller unmarshaller;
	
	BufferedWriter fileWriter;
	BufferedWriter fileIndexWriter;

	private JobID jobID;
	
	public SplitterThread(InstanceSet instanceSet, String configPath, String indexPath, Reporter reporter, JobID jodID) throws JMSException{
		
		this.instanceSet = instanceSet;
		this.configPath = configPath;
		this.indexPath = indexPath;
		this.reporter = reporter;
		this.jobID = jodID;
		
		ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory();
		activeMQConnectionFactory.setBrokerURL("tcp://localhost:61616");
		QueueConnection connection = activeMQConnectionFactory.createQueueConnection();
		computingReportationQueueSession = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		
		ActiveMQQueue activeMQQueue = new ActiveMQQueue("computingReportationQueue");
		
		computingReportationQueueSender = computingReportationQueueSession.createSender(activeMQQueue);
		
		ApplicationContext context = new ClassPathXmlApplicationContext("applicationContextXML.xml");
		CastorMarshaller castorMarshaller = (CastorMarshaller)context.getBean("castorMarshaller");
		this.setMarshaller(castorMarshaller);
		this.setUnmarshaller(castorMarshaller);
	}
	
	public void setMarshaller(Marshaller marshaller) {
		this.marshaller = marshaller;
	}
	
	public void setUnmarshaller(Unmarshaller unmarshaller) {
		this.unmarshaller = unmarshaller;
	}
	
	public void run(){
		
		
		
		FileInputStream is = null;
		FileOutputStream os = null;
		
		try{
			
			//String path = configPath.substring(0, configPath.indexOf("\\config.xml"));
			String path = configPath.substring(0, configPath.indexOf("/config.xml"));
			URI uri = new URI(path);
			File folder = new File(uri);
			folder.mkdirs();
			
			// on passe de Java à XML et juste après de XML à Java
			// cela ne sert à rien !
			// on ne fait que tester la conversion java <-> XML
			uri = new URI(configPath);
			File file = new File(uri);
			//os = new FileOutputStream(configPath);
			os = new FileOutputStream(file);
			this.marshaller.marshal(instanceSet, new StreamResult(os));
			
			//is = new FileInputStream(configPath);
			is = new FileInputStream(file);
			InstanceSet instanceSet = (InstanceSet) this.unmarshaller.unmarshal(new StreamSource(is));
			
			//sending the treatment to a worker
			List<Param> params = instanceSet.getParams();
			
			Param param = params.get(0);
			Iterator<Param> paramIt = params.iterator();
			
			uri = new URI(indexPath+"/splits.txt");
			file = new File(uri);
			//fileWriter = new BufferedWriter(new FileWriter(indexPath+"\\splits.txt"));
			fileWriter = new BufferedWriter(new FileWriter(file));
			
			uri = new URI(indexPath+"/index.txt");
			file = new File(uri);
			//fileIndexWriter = new BufferedWriter(new FileWriter(indexPath + "\\index.txt"));
			fileIndexWriter = new BufferedWriter(new FileWriter(file));
			
			createSplit(param, paramIt, jobID);
			
			fileWriter.close();
			fileIndexWriter.close();
			
			reporter.getTaskStatus().setRunState(TaskStatus.State.SUCCEEDED);
			
			
			logger.debug("Send status split");
			Message message = computingReportationQueueSession.createObjectMessage(reporter);
			computingReportationQueueSender.send(message);
			
		}catch(Exception e){
			e.printStackTrace();
		}finally {
			if (os != null) {
				try {
					os.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (fileWriter != null) {
				try {
					fileWriter.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (fileIndexWriter != null) {
				try {
					fileIndexWriter.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	private void createSplit(Param param, Iterator<Param> paramIt, JobID jobID) throws IOException
	{
		DataIndex dataIndex = new DataIndex(jobID);
		//dataIndex.setOffset(jobID.getId()+"\\splits.txt");
		dataIndex.setOffset(indexPath+"/splits.txt");
		
		if(paramIt.hasNext() == false)
		{
			return;
		}
		Param p = paramIt.next();
		String paramConfigString = p.getParamConfigString();
		String[] paramConfigStrings = paramConfigString.split(":");
		
		int nbline = 0;
		for(int i = Integer.parseInt(paramConfigStrings[0]) ; i <= Integer.parseInt(paramConfigStrings[2]) ; i += Integer.parseInt(paramConfigStrings[1]))
		{
			
			fileWriter.write(p.getParamName()+"="+i);
			dataIndex.setLength(String.valueOf(i).length());
			fileWriter.newLine();
			dataIndex.setRow(nbline);
			fileIndexWriter.write("Offset="+dataIndex.getOffset()+" Row="+dataIndex.getRow()+" Column="+dataIndex.getColumn()+" Lenght="+dataIndex.getLength());
			fileIndexWriter.newLine();
			createSplit(p, paramIt, jobID);	
			nbline++;
		}
	}
}