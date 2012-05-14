package edu.washington.cs.sqlsynth.entity;

import edu.washington.cs.sqlsynth.entity.QueryCondition.CONJ;

public class ConditionNode {
	
	enum OP{GT, EQ, LT}; //greater than, equal, or less than

	private final boolean isLeaf;
	
	//if it is a condition node
	private CONJ conj = null;
	private TableColumn leftColumn = null;
	private TableColumn rightColumn = null;
	private Object rightConstant = null; //constant is always on the right side
	
	//if it is not a condition node
	private OP op = null;
	private ConditionNode left = null;
	private ConditionNode right = null;
	
	public ConditionNode(boolean isLeaf) {
		this.isLeaf = isLeaf;
	}
}