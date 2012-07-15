package test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoOperations;

import com.runmycode.mapred.Mapper;
import com.runmycode.mapred.mongoDB.MongoDBFactory;

class Person 
{	
	Mapper object;
	
	public Class<? extends Mapper> getMapper()
	{
		return object.getClass();
	}
	
	public void setClass( Class<? extends Mapper>  classe) throws InstantiationException, IllegalAccessException
	{
		object = classe.newInstance();
	}
	
	@Id
	private String id;
	private String name;
	private int age;
	
	public Person(String name, int age, String id) 
	{
		this.name = name;
		this.age = age;
		this.id = id;
	}
	
	public String getId()
	{
		return id;
	}
	
	public String getName()
	{
		return name;
	}
	
	public int getAge() 
	{
		return age;
	}
	
	@Override
	public String toString()
	{
		return "Person [id=" + id + ", name=" + name + ", age=" + age + "]";
	}
}

class Ecole
{
	List<String> personIDs = new ArrayList<String>();
	
	public void addPersonID(Person p)
	{
		personIDs.add(p.getId());		
	}
}

public class MainMongoDBTest 
{
	public static void main(String[] args) 
	{    	
		MongoOperations mongoOperation = MongoDBFactory.getMongoOperations();
		
		mongoOperation.dropCollection("classes");
		
		/*
		Ecole ecole = new Ecole();
		Person p = new Person("efb", 23, "id1");
		try {
			p.setClass(DefaultMapper1.class);
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//Class classe = p.getClass();
	
		mongoOperation.insert(p, "classes");
		
		Person p1 = mongoOperation.findById("id1", Person.class, "classes");
		
		System.out.println(p1.toString());
		System.out.println(p1.getMapper());
		System.out.println(p1.object);
		
		mongoOperation.save(classe, "classes");
		//*/
		
		
		mongoOperation.dropCollection(Ecole.class);
		mongoOperation.dropCollection(Person.class);
		
		/*
		Person p1 = new Person("Joe", 34, "1");
		Person p2 = new Person("Toto", 35, "2");
		
		Ecole ec = new Ecole();
		
		ec.addPersonID(p1);
		ec.addPersonID(p2);
		
		mongoOperation.insert(p1);
		mongoOperation.insert(p2);
		
		mongoOperation.insert(ec);
		
		mongoOperation.updateFirst(Query.query(Criteria.where("_id").is("1")), Update.update("age", 2), Person.class);
		
		Person person = mongoOperation.findOne(Query.query(Criteria.where("_id").is("1")), Person.class);
		
		System.out.println(person.toString());
		
		List<Person> ps = mongoOperation.find(Query.query(Criteria.where("personIDs").is("1")), Person.class, "ecole");
		
		System.out.println(ps.size());
			
		//insert		
		mongoOperation.insert(p);
		System.out.println("Insert : " + p);
		
		//find		
		p = mongoOperation.findById(p.getId(), Person.class);
		System.out.println("Found : " + p);
		
		//update		
		mongoOperation.updateFirst(Query.query(Criteria.where("name").is("Joe")),Update.update("age", 35), Person.class);
		p = mongoOperation.findOne(Query.query(Criteria.where("name").is("Joe")), Person.class);
		System.out.println("Updated : " + p);
		
		//delete ?		
		mongoOperation.remove(p);
		        		        
		//check that deletion worked	
		List<Person> people = mongoOperation.findAll(Person.class);		        
		System.out.println("Number of people = " + people.size());
		//*/
		
		mongoOperation.dropCollection(Person.class);
		
		/*try 
		{					
			mongoOperation.dropCollection(JobConf.class);
									
			JobConf job = new JobConf();
	
			job.setJobName("script1");
			job.setUser("utilisateur1");
			
			FileInputFormat.setInputPaths(job, "/Demandes");
			FileOutputFormat.setOutputPath(job, "/Resultats");
		
			job.setMapperClass(PlateformEmulatorMapper.class);
			job.setReducerClass(ResultAgregationReduce.class); 	
		
			job.setInputFormat(MongoDBInputFormat.class);
			job.setOutputFormat(TextOutputFormat.class);

			System.out.println(job.getJobID().getId());
			
			mongoOperation.insert(job);
			
			mongoOperation.dropCollection(JobInProgress.class);
						
			JobConf jobConf = mongoOperation.findById("idMembreidJobidSite", JobConf.class);
			
			System.out.println(jobConf.getUser());
			System.out.println(jobConf.getNumMapTasks());
			
			JobInProgress jobInProgress = new JobInProgress(jobConf.getJobID());
			mongoOperation.insert(jobInProgress, "jobInProgress");
			
			mongoOperation.dropCollection(TaskInProgress.class);
			mongoOperation.dropCollection(Task.class);
			
			TaskInProgress taskInProgress = new TaskInProgress(jobConf.getJobID());
			
			TaskID taskID = TaskID.forName("idMembre_idJob_idSite_idTask");
			
			Task task = taskInProgress.addRunningTask(taskID);
			
			mongoOperation.insert(taskInProgress);
			
			mongoOperation.insert(task);
			
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}*/
		
		try 
		{
			mongoOperation.dropCollection(File.class);
			
			File fichier = new File("toto.txt");
			BufferedWriter writer = new BufferedWriter(new FileWriter(fichier));
			writer.write("Hello World !");
			writer.close();
			
			mongoOperation.insert(fichier);
			
			fichier.delete();
			
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
	}		    	
}
