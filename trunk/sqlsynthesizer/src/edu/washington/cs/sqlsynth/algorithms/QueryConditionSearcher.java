package edu.washington.cs.sqlsynth.algorithms;

import java.util.Collection;

import edu.washington.cs.sqlsynth.entity.QueryCondition;

public class QueryConditionSearcher {

	public final SQLQueryCompletor completor;
	
	public QueryConditionSearcher(SQLQueryCompletor completor) {
		this.completor = completor;
	}
	
	public Collection<QueryCondition> inferQueryConditions() {
		//use decision tree to infer query condition
		throw new RuntimeException("");
	}
}
