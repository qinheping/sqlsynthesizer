package edu.washington.cs.sqlsynth.algorithms;

import java.util.List;
import java.util.Map;

import edu.washington.cs.sqlsynth.entity.AggregateExpr;
import edu.washington.cs.sqlsynth.entity.TableColumn;

public class AggregateExprInfer {

public final SQLQueryCompletor completor;
	
	public AggregateExprInfer(SQLQueryCompletor completor) {
		this.completor = completor;
	}
	
	public Map<Integer, AggregateExpr> inferAggregationExprs() {
		throw new RuntimeException();
	}
	
	public List<TableColumn> inferGroupbyColumns() {
		throw new RuntimeException();
	}
}
