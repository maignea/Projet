package com.runmycode.ro;

import java.io.Serializable;

public class Param implements Serializable
{	
	public enum ParamDim
	{
		SCALAR,
		VECTOR
	}
	
	public enum ParamType
	{
		INTEGER
	}
	
	private String paramName;
	private ParamDim paramDim;
	private ParamType paramType;
	private String paramConfigString;
	
	private Constraint constraint;
	
	public Param(){
	}
	
	public Param(String paramName, ParamDim paramDim, ParamType paramType, String paramConfigString, Constraint constraint)
	{
		this.paramName = paramName;
		this.paramType = paramType;
		this.paramConfigString = paramConfigString;
		this.constraint = constraint;
	}

	public String getParamName() 
	{
		return paramName;
	}

	public void setParamName(String paramName) 
	{
		this.paramName = paramName;
	}

	public ParamDim getParamDim() 
	{
		return paramDim;
	}

	public void setParamDim(ParamDim paramDim) 
	{
		this.paramDim = paramDim;
	}

	public ParamType getParamType() 
	{
		return paramType;
	}

	public void setParamType(ParamType paramType) 
	{
		this.paramType = paramType;
	}

	public String getParamConfigString() 
	{
		return paramConfigString;
	}

	public void setParamConfigString(String paramConfigString) 
	{
		this.paramConfigString = paramConfigString;
	}

	public Constraint getConstraint() 
	{
		return constraint;
	}

	public void setConstraint(Constraint constraint) 
	{
		this.constraint = constraint;
	}

	@Override
	public int hashCode() 
	{
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((constraint == null) ? 0 : constraint.hashCode());
		result = prime
				* result
				+ ((paramConfigString == null) ? 0 : paramConfigString
						.hashCode());
		result = prime * result
				+ ((paramDim == null) ? 0 : paramDim.hashCode());
		result = prime * result
				+ ((paramName == null) ? 0 : paramName.hashCode());
		result = prime * result
				+ ((paramType == null) ? 0 : paramType.hashCode());
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
		Param other = (Param) obj;
		if (constraint == null) 
		{
			if (other.constraint != null)
				return false;
		}
		else if (!constraint.equals(other.constraint))
			return false;
		if (paramConfigString == null) {
			if (other.paramConfigString != null)
				return false;
		}
		else if (!paramConfigString.equals(other.paramConfigString))
			return false;
		if (paramDim != other.paramDim)
			return false;
		if (paramName == null) 
		{
			if (other.paramName != null)
				return false;
		}
		else if (!paramName.equals(other.paramName))
			return false;
		if (paramType != other.paramType)
			return false;
		return true;
	}
}
