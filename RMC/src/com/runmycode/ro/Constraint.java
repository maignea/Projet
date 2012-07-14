package com.runmycode.ro;

import java.io.Serializable;

public class Constraint implements Serializable
{	
	public enum ConstraintType
	{
		VECTOR_SIZE_DEFINED_BY
	}
	
	private String constraintValue;
	private ConstraintType constraintType;
	private String constraintOrigin;
	
	public Constraint(String constraintValue, ConstraintType constraintType, String constraintOrigin) 
	{
		super();
		this.constraintValue = constraintValue;
		this.constraintType = constraintType;
		this.constraintOrigin = constraintOrigin;
	}

	public String getConstraintValue() 
	{
		return constraintValue;
	}

	public void setConstraintValue(String constraintValue) 
	{
		this.constraintValue = constraintValue;
	}

	public ConstraintType getConstraintType() 
	{
		return constraintType;
	}

	public void setConstraintType(ConstraintType constraintType) 
	{
		this.constraintType = constraintType;
	}

	public String getConstraintOrigin() 
	{
		return constraintOrigin;
	}

	public void setConstraintOrigin(String constraintOrigin) 
	{
		this.constraintOrigin = constraintOrigin;
	}	
}