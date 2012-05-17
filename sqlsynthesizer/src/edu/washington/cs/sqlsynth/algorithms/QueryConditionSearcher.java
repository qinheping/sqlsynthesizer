package edu.washington.cs.sqlsynth.algorithms;

import java.util.Collection;
import java.util.Collections;

import edu.washington.cs.sqlsynth.entity.QueryCondition;

// firstly, use simpely weka
import weka.core.Instances;
import weka.classifiers.trees.j48.*;

public class QueryConditionSearcher {

	public final SQLQueryCompletor completor;
	
	public QueryConditionSearcher(SQLQueryCompletor completor) {
		this.completor = completor;
	}
	
	public Collection<QueryCondition> inferQueryConditions() {
		//use decision tree to infer query condition
		//throw new RuntimeException("");
		return Collections.emptySet();
	}
}
