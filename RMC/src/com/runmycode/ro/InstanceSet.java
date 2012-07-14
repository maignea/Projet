package com.runmycode.ro;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

public class InstanceSet implements Serializable
{
	private int N;
	
	private List<Param> params = new ArrayList<Param>();
	
	public InstanceSet(){
	}
	
	public InstanceSet(int N)
	{
		this.N = N;
	}
	
	public void addParam(Param param)
	{
		params.add(param);
	}

	public List<Param> getParams() 
	{
		return params;
	}

	public int getN() {
		return N;
	}

	public void setN(int n) {
		N = n;
	}

	public void setParams(List<Param> params) {
		this.params = params;
	}
	
	
	
}
