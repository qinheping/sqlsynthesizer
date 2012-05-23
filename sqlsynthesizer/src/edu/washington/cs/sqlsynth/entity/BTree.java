package edu.washington.cs.sqlsynth.entity;

import java.util.LinkedList;
//import java.util.ArrayDeque;

public class BTree {

	BNode theBTRootNode;
	
	public BTree()
	{
		this.theBTRootNode = null;	
	}
	
	private void insertBNode(BNode theRootNode, BNode newNode, boolean isLeft)
	{
		if (theRootNode == null)
		{
			this.theBTRootNode = newNode;
		}
		else if(isLeft)
		{
			theRootNode.addLeft(newNode);
		}
		else
		{
			theRootNode.addRight(newNode);
		}
	}
	
	public void buildTreeFromRules(String[] allRules, int[] allLabels)
	{
		if (allRules.length != allLabels.length)
		{
			System.err.println("---------------------Something Wrong in BuildTreeFromRules---------------------");
		}
		BNode root = new BNode("", -1);
		this.insertBNode(this.theBTRootNode, root, true);
		for (int i = 0; i<allRules.length-1; ++i)
		{
			BNode leftNode = new BNode(allRules[i], allLabels[i]);
			BNode rightNode = null;
			if (i == allRules.length-2)
			{
				rightNode = new BNode("NOT ("+allRules[i]+")", allLabels[i+1]);
			}
			else
			{
				rightNode = new BNode("NOT ("+allRules[i]+")", -1);
			}
			insertBNode(root, leftNode, true);
			insertBNode(root, rightNode, false);
			root = rightNode;
		}
		
		System.out.println("------------------End of buildTreeFromRules------------------");
	}
	
	public void getRulesFromTree()
	{
		System.out.print(getRulesFromTree(this.theBTRootNode, ""));
	}
	
	public String getRulesFromTree(BNode root, String prefix)
	{
		if (root == null)
		{
			return "";
		}
		if (root.getLeft() == null && root.getRight() == null)
		{
			if (root.getLabel() == 1)
			{
				return prefix + "\n";
			}
			else
			{
				return "";
			}
		}
		else
		{
		
			if (prefix.length() == 0)
			{
				return getRulesFromTree(root.getLeft(), root.getLeft().toRule()) + getRulesFromTree(root.getRight(), root.getRight().toRule());
			}
			else
			{
				return getRulesFromTree(root.getLeft(), "("+prefix+") AND ("+root.getLeft().toRule()+")") + getRulesFromTree(root.getRight(), "("+prefix+") AND ("+root.getRight().toRule()+")");
			}
		}
	}
}
