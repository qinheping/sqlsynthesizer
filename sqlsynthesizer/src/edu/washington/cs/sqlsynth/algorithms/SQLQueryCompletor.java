package edu.washington.cs.sqlsynth.algorithms;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.washington.cs.sqlsynth.db.DbConnector;
import edu.washington.cs.sqlsynth.entity.AggregateExpr;
import edu.washington.cs.sqlsynth.entity.QueryCondition;
import edu.washington.cs.sqlsynth.entity.SQLQuery;
import edu.washington.cs.sqlsynth.entity.SQLSkeleton;
import edu.washington.cs.sqlsynth.entity.TableColumn;
import edu.washington.cs.sqlsynth.entity.TableInstance;
import edu.washington.cs.sqlsynth.util.Maths;
import edu.washington.cs.sqlsynth.util.Utils;



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
		Map<Integer, List<AggregateExpr>> aggrExprs = aggInfer.inferAggregationExprs();
		List<TableColumn> groupbyColumns = aggInfer.inferGroupbyColumns();
		
		//create SQL statements
		
		List<SQLQuery> quries = new LinkedList<SQLQuery>();
//		quries.add(new SQLQuery(skeleton)); //FIXME incomplete
		quries.addAll(constructQueries(skeleton, aggrExprs, groupbyColumns));
		return quries;
	}
	
	List<SQLQuery> constructQueries(SQLSkeleton skeleton, Map<Integer, List<AggregateExpr>> aggrExprs, List<TableColumn> groupbyColumns) {
		if(aggrExprs.isEmpty()) {
			Utils.checkTrue(groupbyColumns.isEmpty()); //no aggregation, no group by
			return Collections.singletonList(new SQLQuery(skeleton));
		}
		List<SQLQuery> queries = new LinkedList<SQLQuery>();
		//construct
		
//		Utils.checkTrue(aggrExprs.size() == 1); //FIXME
		List<Integer> keyList = new LinkedList<Integer>();
		List<List<AggregateExpr>> exprList = new LinkedList<List<AggregateExpr>>();
		for(Integer key : aggrExprs.keySet()) {
			keyList.add(key);
			exprList.add(aggrExprs.get(key));
		}
		List<List<AggregateExpr>> allCombinations = Maths.allCombination(exprList);
		for(List<AggregateExpr> list : allCombinations) {
			Utils.checkTrue(list.size() == keyList.size());
			Map<Integer, AggregateExpr> map = new LinkedHashMap<Integer, AggregateExpr>();
			for(int i = 0; i < keyList.size(); i++) {
				map.put(keyList.get(i), list.get(i));
			}
			SQLQuery q = new SQLQuery(skeleton);
			q.setAggregateExprs(map);
			q.setGroupbyColumns(groupbyColumns);
			queries.add(q);
		}
		
		//replicate the aggr exprs
//		int key = aggrExprs.keySet().iterator().next();
//		for(AggregateExpr ex : aggrExprs.get(key)) {
//			SQLQuery q = new SQLQuery(skeleton);
//			Map<Integer, AggregateExpr> map = Collections.singletonMap(key, ex);
//			q.setAggregateExprs(map);
//			q.setGroupbyColumns(groupbyColumns);
//			queries.add(q);
//		}
		return queries;
	}
	
	public List<SQLQuery> validateQueriesOnDb(Collection<SQLQuery> qs) {
		List<SQLQuery> queries = new LinkedList<SQLQuery>();
		DbConnector connector = DbConnector.instance();
		for(SQLQuery query : qs) {
			connector.initializeInputTables(inputTables);
			boolean checked = connector.checkSQLQueryWithOutput(outputTable, query);
			if(checked) {
				queries.add(query);
			}
		}
		return queries;
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