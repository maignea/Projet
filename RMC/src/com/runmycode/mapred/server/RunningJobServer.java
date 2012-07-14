package com.runmycode.mapred.server;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.data.mongodb.core.MongoOperations;

import com.runmycode.mapred.Counters;
import com.runmycode.mapred.JobConf;
import com.runmycode.mapred.JobID;
import com.runmycode.mapred.JobInProgress;
import com.runmycode.mapred.JobPriority;
import com.runmycode.mapred.JobStatus;
import com.runmycode.mapred.RunningJob;
import com.runmycode.mapred.TaskCompletionEvent;
import com.runmycode.mapred.TaskID;
import com.runmycode.mapred.TaskInProgress;
import com.runmycode.mapred.TaskReport;
import com.runmycode.mapred.TaskStatus.State;
import com.runmycode.mapred.mongoDB.MongoDBFactory;

public class RunningJobServer implements RunningJob 
{
	private transient MongoOperations mongoOperations = MongoDBFactory.getMongoOperations();
	private transient Logger logger = Logger.getLogger(com.runmycode.mapred.server.RunningJobServer.class);
	
	private JobID jobID;
	
	private JobConf getJobConf()
	{
		return mongoOperations.findById(jobID.getId(), JobConf.class, "jobConf");
	}
	
	private JobInProgress getJobInProgress()
	{
		return mongoOperations.findById(jobID.getId(), JobInProgress.class, "jobInProgress");
	}
	
	/*class JMSServerMessageListener implements MessageListener{
		
		JobClient_ServerSide jobClient;
		
		RunningJobServer runningJobServer;
		
		JobID jobID;
		//Queue queue;
		QueueConnection connection;
		QueueSession session;
		//QueueSender queueSender;
		QueueReceiver queueReceiver;
		
		public JMSServerMessageListener(RunningJobServer runningJobServer, QueueConnectionFactory connectionFactory, Queue receivingQueue) throws JMSException {
			super();
			this.runningJobServer = runningJobServer;
			//this.queue = queue;
			connection = connectionFactory.createQueueConnection();
			session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
			//queueSender = session.createSender(sendingQueue);
			queueReceiver = session.createReceiver(receivingQueue);
			queueReceiver.setMessageListener(this);
			connection.start();
		}

		public void setJobID(JobID jobID) {
			this.jobID = jobID;
		}

	    public void setJobClient(JobClient_ServerSide jobClient) {
			this.jobClient = jobClient;
		}

		public void onMessage( Message message ){ 
			
			System.out.println("on Message : " + message);
			
	    	if (message instanceof ObjectMessage){
	    		try {
	    			
	    			ObjectMessage objectMessage = (ObjectMessage)message;
	    			RunningJobMessageClientToServer runningJobMessage = (RunningJobMessageClientToServer)objectMessage.getObject();
	    			
	    			System.out.println("on Message : " + runningJobMessage);
	    			
	    			JobID jobID = runningJobMessage.getJobID();
	    		
					Class runningJobClass = runningJobServer.getClass();
					
					String methodName = runningJobMessage.getMethodName();
					Object[] methodParameters = runningJobMessage.getMethodParameters();
					Method[] methods = runningJobClass.getDeclaredMethods();
					int i=0;
					while(i<methods.length && methods[i].getName().equals(methodName)==false){
						i++;
					}
					Method method;
					if(i < methods.length){
						method = methods[i];
						Object returnValue = method.invoke(runningJobServer, methodParameters);
						
					} else {
						
					}
					
					
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
	    		
	    		
	    	}
	    }
	    
	}


	JMSServerMessageListener jmsServerMessageListener;
	QueueConnection connection;
	QueueSession session;
	QueueSender queueSender;
	
	public RunningJobServer(QueueConnectionFactory connectionFactory, Queue sendingQueue, Queue receivingQueue) throws JMSException {
		jmsServerMessageListener = new JMSServerMessageListener(this, connectionFactory, receivingQueue);
		connection = connectionFactory.createQueueConnection();
		session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		queueSender = session.createSender(sendingQueue);
	}*/

	public RunningJobServer(JobID jobID) 
	{
		this.jobID = jobID;
	}

	@Override
	public JobID getID() 
	{	
		return this.jobID;
	}

	@Override
	public String getJobName() 
	{		
		logger.debug("getJobName dans RunningJobServer");
		
		return getJobConf().getJobName();
	}

	@Override
	public float getMapProgress() throws IOException 
	{
		return getJobInProgress().getStatus().mapProgress();
	}

	@Override
	public float getReduceProgress() throws IOException 
	{
		return getJobInProgress().getStatus().reduceProgress();	
	}

	@Override
	public Set<TaskCompletionEvent> getTaskCompletionEvents(int startFrom) throws IOException 
	{	
		return getJobInProgress().getTaskCompletionEvents(startFrom, 10);
	}

	@Override
	public boolean isComplete() throws IOException 
	{
		return (getJobInProgress().getStatus().getRunState() == JobStatus.State.SUCCEEDED || getJobInProgress().getStatus().getRunState() == JobStatus.State.KILLED || getJobInProgress().getStatus().getRunState() == JobStatus.State.FAILED);
	}

	@Override
	public boolean isSuccessful() throws IOException 
	{
		return (getJobInProgress().getStatus().getRunState() == JobStatus.State.SUCCEEDED);
	}

	@Override
	public void waitForCompletion() throws IOException 
	{
		while(!isComplete())
		{
			
		}
	}

	@Override
	public JobStatus getJobState() throws IOException 
	{
		return getJobInProgress().getStatus();
	}

	@Override
	public void killJob() throws IOException 
	{
		getJobInProgress().kill();		
	}

	@Override
	public void setJobPriority(String priority) throws IOException 
	{
		JobPriority jobPriority = JobPriority.valueOf(priority);
		
		getJobInProgress().setPriority(jobPriority);				
	}

	@Override
	public void killTask(TaskID taskId, boolean shouldFail) throws IOException 
	{	
		getJobInProgress().getTaskInProgress(taskId).kill();
	}

	@Override
	public Counters getCounters() throws IOException 
	{
		return getJobInProgress().getJobCounters();
	}

	@Override
	public String getFailureInfo() throws IOException 
	{
		return getJobInProgress().getStatus().getFailureInfo();
	}

	@Override
	public Set<String> getTaskDiagnostics(TaskID taskid) throws IOException 
	{
		Set<String> diagnostics = new HashSet<String>();
		diagnostics.add("test diagnostic1");
		return diagnostics;
	}

	public void updateStatus(State taskStatus) 
	{
		
	}
	
	@Override
	public Set<TaskReport> getMapTaskReports() throws IOException 
	{
		TaskInProgress taskInProgress;
		JobInProgress jobInProgress = this.getJobInProgress(); 
		List<TaskInProgress> mapTasks = jobInProgress.getMapTasks();
		HashSet<TaskReport> reports = new HashSet<TaskReport>(); 
		for(int i = 0 ; i < mapTasks.size() ; i++)
		{
			taskInProgress = mapTasks.get(i);
			reports.add(taskInProgress.generateSingleReport());
		}
		return reports;
	}

	@Override
	public Set<TaskReport> getReduceTaskReports() 
	{
		TaskInProgress taskInProgress;
		JobInProgress jobInProgress = this.getJobInProgress(); 
		List<TaskInProgress> reduceTasks = jobInProgress.getReduceTasks();
		HashSet<TaskReport> reports = new HashSet<TaskReport>(); 
		for(int i=0; i<reduceTasks.size(); i++)
		{
			taskInProgress = reduceTasks.get(i);
			reports.add(taskInProgress.generateSingleReport());
		}
		return reports;
	}

	@Override
	public Set<TaskReport> getSplitTaskReports() 
	{
		TaskInProgress taskInProgress;
		JobInProgress jobInProgress = this.getJobInProgress(); 
		List<TaskInProgress> reduceTasks = jobInProgress.getSplitTasks();
		HashSet<TaskReport> reports = new HashSet<TaskReport>(); 
		for(int i=0; i<reduceTasks.size(); i++)
		{
			taskInProgress = reduceTasks.get(i);
			reports.add(taskInProgress.generateSingleReport());
		}
		return reports;
	}

	@Override
	public long getStartTime() {
		return getJobInProgress().getStartTime();
	}

	@Override
	public long getLaunchTime() {
		return getJobInProgress().getLaunchTime();
	}

	@Override
	public long getFinishTime() {
		return getJobInProgress().getFinishTime();
	}

}
