package edu.washington.cs.sqlsynth.entity;

import edu.washington.cs.sqlsynth.util.Utils;

public class QueryCondition {
	
	enum CONJ {
		AND { public String toString() { return " and "; }},
		OR { public String toString() { return " or "; }}
		};
	
	private final ConditionNode root;
	
	public QueryCondition(ConditionNode root) {
		Utils.checkNotNull(root);
		this.root = root;
	}
	
	public String toSQLCode() {
		return root.toSQLString();
	}
	
}
