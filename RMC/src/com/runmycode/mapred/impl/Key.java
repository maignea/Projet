package com.runmycode.mapred.impl;

import com.runmycode.ro.Param;

public class Key 
{
	private Param param;
	
	public void setParam(Param param)
	{
		this.param = param;
	}
	
	@Override
	public int hashCode() 
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((param == null) ? 0 : param.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) 
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Key other = (Key) obj;
		if (param == null) 
		{
			if (other.param != null)
				return false;
		} 
		else if (!param.equals(other.param))
			return false;
		return true;
	}

	public Param getParam() 
	{
		return param;
	}
	
	@Override
	public String toString() 
	{
		return param.getParamConfigString();
	}
}
