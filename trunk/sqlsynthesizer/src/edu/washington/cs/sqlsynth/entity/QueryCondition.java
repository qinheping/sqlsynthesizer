package edu.washington.cs.sqlsynth.entity;

import edu.washington.cs.sqlsynth.util.Utils;

public class QueryCondition {
	
	enum CONJ {AND, OR};
	
	private final ConditionNode root;
	
	public QueryCondition(ConditionNode root) {
		Utils.checkNotNull(root);
		this.root = root;
	}
	
	public String toSQLCode() {
		throw new RuntimeException();
	}
	
}
