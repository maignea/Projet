package com.runmycode.mapred.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.Calendar;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.QueueConnection;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;

import com.runmycode.mapred.DataIndex;
import com.runmycode.mapred.Key;
import com.runmycode.mapred.Reporter;
import com.runmycode.mapred.TaskStatus;
import com.runmycode.ro.Param;

public class MapperThread extends Thread{
	
	private String outputPath;
	private Key key;
	public DataIndex value;
	private Reporter reporter;
	private QueueSession computingReportationQueueSession;
	private QueueSender computingReportationQueueSender;

	public MapperThread(String outputPath, Key key, DataIndex value, Reporter reporter) throws JMSException{
		
		this.outputPath = outputPath;
		this.key = key;
		this.value = value;
		this.reporter = reporter;
		ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory();
		activeMQConnectionFactory.setBrokerURL("tcp://localhost:61616");
		QueueConnection connection = activeMQConnectionFactory.createQueueConnection();
		computingReportationQueueSession = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		
		ActiveMQQueue activeMQQueue = new ActiveMQQueue("computingReportationQueue");
		
		computingReportationQueueSender = computingReportationQueueSession.createSender(activeMQQueue);
	}
	
	public void run(){
		
		try{
			
		MapCollector output = new MapCollector(outputPath);
		output.open();
		
		URI uri = new URI(value.getOffset());
		File file = new File(uri);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		//BufferedReader reader = new BufferedReader(new FileReader(value.getOffset()));
		
		String line = null;

		//System.out.println("value       = " + value.getRow());
		
		for(int i = 0 ; i <= value.getRow() ; i++)
		{		
			line = reader.readLine();
			//System.out.println("line       = " + line);
		}					
		
		int dataValue = Integer.parseInt(line.substring(line.indexOf("=")+1));
		
		//treatment execution
		int result = dataValue + dataValue;
		//writing results on filesystem
		int day = Calendar.DAY_OF_MONTH;
		
		com.runmycode.mapred.impl.Key outputKey_1 = new com.runmycode.mapred.impl.Key();
		Param param1 = new Param("result", Param.ParamDim.SCALAR, Param.ParamType.INTEGER, String.valueOf(day), null);
		outputKey_1.setParam(param1);
		
		day++;
		com.runmycode.mapred.impl.Key outputKey_2 = new com.runmycode.mapred.impl.Key();
		Param param2 = new Param("result", Param.ParamDim.SCALAR, Param.ParamType.INTEGER, String.valueOf(day), null);
		outputKey_2.setParam(param2);
		
		output.collect(outputKey_1, result);
		output.collect(outputKey_2, result);
		
		reporter.getTaskStatus().setRunState(TaskStatus.State.SUCCEEDED);
		
		int sleep = (int)(1000*Math.random());
		Thread.sleep(sleep);

		Message message = computingReportationQueueSession.createObjectMessage(reporter);
		computingReportationQueueSender.send(message);
		
		output.close();
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}