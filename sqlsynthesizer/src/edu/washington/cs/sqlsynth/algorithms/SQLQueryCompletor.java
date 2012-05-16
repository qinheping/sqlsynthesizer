package edu.washington.cs.sqlsynth.algorithms;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.washington.cs.sqlsynth.entity.AggregateExpr;
import edu.washington.cs.sqlsynth.entity.QueryCondition;
import edu.washington.cs.sqlsynth.entity.SQLQuery;
import edu.washington.cs.sqlsynth.entity.SQLSkeleton;
import edu.washington.cs.sqlsynth.entity.TableColumn;
import edu.washington.cs.sqlsynth.entity.TableInstance;

public class SQLQueryCompletor {

	private final SQLSkeleton skeleton;
	private List<TableInstance> inputTables = new LinkedList<TableInstance>();
	private TableInstance outputTable = null;
	
	public SQLQueryCompletor(SQLSkeleton skeleton) {
		this.skeleton = skeleton;
	}

	public List<SQLQuery> inferSQLQueries() {
		//it calls QueryConditionSearcher to infer conditions
		QueryConditionSearcher searcher = new QueryConditionSearcher(this);
		Collection<QueryCondition> conditions = searcher.inferQueryConditions();
		
		AggregateExprInfer aggInfer = new AggregateExprInfer(this);
		Map<Integer, AggregateExpr> aggrExprs = aggInfer.inferAggregationExprs();
		List<TableColumn> groupbyColumns = aggInfer.inferGroupbyColumns();
		
		//create SQL statements
		
		throw new RuntimeException();
	}
	
	public SQLSkeleton getSkeleton() {
		return skeleton;
	}

	public List<TableInstance> getInputTables() {
		return inputTables;
	}
	
	public void addInputTable(TableInstance inputTable) {
		this.inputTables.add(inputTable);
	}

	public TableInstance getOutputTable() {
		return outputTable;
	}

	public void setOutputTable(TableInstance outputTable) {
		this.outputTable = outputTable;
	}
}