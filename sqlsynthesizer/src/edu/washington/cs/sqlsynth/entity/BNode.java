package edu.washington.cs.sqlsynth.entity;

public class BNode {
	
	private BNode leftBNode, rightBNode;
	private String parentRule;
	private int label;
	
	public BNode(String parentRule, int label)
	{
		this.parentRule = parentRule;
		this.leftBNode = null;
		this.rightBNode = null;
		this.label = label;
	}
	
	public String toRule()
	{
		return this.parentRule;
	}
		
	public BNode getLeft()
	{
		return this.leftBNode;
	}
	
	public BNode getRight()
	{
		return this.rightBNode;
	}
	
	public int getLabel()
	{
		return this.label;
	}
	
	public void addLeft(BNode newNode)
	{
		this.leftBNode = newNode;
	}
	
	public void addRight(BNode newNode)
	{
		this.rightBNode = newNode;
	}

}
