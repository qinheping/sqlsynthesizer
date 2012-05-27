package edu.washington.cs.sqlsynth.algorithms;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import plume.Pair;

import edu.washington.cs.sqlsynth.db.DbConnector;
import edu.washington.cs.sqlsynth.entity.AggregateExpr;
import edu.washington.cs.sqlsynth.entity.QueryCondition;
import edu.washington.cs.sqlsynth.entity.SQLQuery;
import edu.washington.cs.sqlsynth.entity.SQLSkeleton;
import edu.washington.cs.sqlsynth.entity.TableColumn;
import edu.washington.cs.sqlsynth.entity.TableInstance;
import edu.washington.cs.sqlsynth.util.Log;
import edu.washington.cs.sqlsynth.util.Maths;
import edu.washington.cs.sqlsynth.util.TableUtils;
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
		//FIXME it is only 1 condition now
		Utils.checkTrue(conditions.size() <= 1, "size: " + conditions.size());
		QueryCondition select = null;
		QueryCondition having = null;
		
		if(!conditions.isEmpty()) {
		    QueryCondition q = Utils.getFirst(conditions);
		    Collection<Pair<QueryCondition, QueryCondition>> pairs = q.splitSelectionAndQueryConditions();
		    Pair<QueryCondition, QueryCondition> p = Utils.getFirst(pairs);
		    select = p.a;
		    having = p.b;
		}
		
		//infer aggregates, and group by columns
		AggregateExprInfer aggInfer = new AggregateExprInfer(this);
		Map<Integer, List<AggregateExpr>> aggrExprs = aggInfer.inferAggregationExprs();
		List<TableColumn> groupbyColumns = aggInfer.inferGroupbyColumns();
		
		//if there is having, but no group, we need to add some group by columns
		if(groupbyColumns.isEmpty() && having != null) {
			if(skeleton.getOutputTable().getColumnNum() == 1) {
				TableColumn c = skeleton.getOutputTable().getColumns().get(0);
				c = TableUtils.findFirstMatchedColumn(c.getColumnName(), skeleton.getTables());
				//System.out.println(c);
				//System.out.println(groupbyColumns.getClass());
				groupbyColumns.add(c);
			}
		}
		
		//create SQL statements
		List<SQLQuery> quries = new LinkedList<SQLQuery>();
		
		
		quries.addAll(constructQueries(skeleton, aggrExprs, groupbyColumns));
		
		List<SQLQuery> replicatedQueries = new LinkedList<SQLQuery>();
		
		for(SQLQuery query : quries) {
			//replicate the query
			SQLQuery repQuery1 = SQLQuery.clone(query);
			SQLQuery repQuery2 = SQLQuery.clone(query);
			if(select != null) {
			    repQuery1.setCondition(select);
			}
			if(having != null) {
				repQuery2.setHavingCondition(having);
			}
			repQuery1.addUnionQuery(repQuery2);
			System.err.println(repQuery1.toSQLString());
			replicatedQueries.add(repQuery1);
//			System.err.println(repQuery2.toSQLString());
			
			
			//the other query
			if(select != null) {
			    query.setCondition(select);
			}
			if(having != null) {
			    query.setHavingCondition(having);
			}
		}
		
		quries.addAll(replicatedQueries);
		
		return quries;
	}
	
//	private List<SQLQuery> constructQueries(SQLSkeleton skeleton, Map<Integer, List<AggregateExpr>> aggrExprs, List<TableColumn> groupbyColumns,
//			Collection<Pair<QueryCondition, QueryCondition>> pairs) {
//		//create SQL statements
//		List<SQLQuery> quries = new LinkedList<SQLQuery>();
//		quries.addAll(constructQueries(skeleton, aggrExprs, groupbyColumns));
//		
//		for(SQLQuery query : quries) {
//			if(select != null) {
//			    query.setCondition(select);
//			}
//			if(having != null) {
//			    query.setHavingCondition(having);
//			}
//		}
//		
//		return quries;
//	}
	
	List<SQLQuery> constructQueries(SQLSkeleton skeleton, Map<Integer, List<AggregateExpr>> aggrExprs, List<TableColumn> groupbyColumns) {
		//may have case: select name where group by x where count(y) > 5
//		if(aggrExprs.isEmpty()) {
//			Utils.checkTrue(groupbyColumns.isEmpty()); //no aggregation, no group by
//			return Collections.singletonList(new SQLQuery(skeleton));
//		}
		List<SQLQuery> queries = new LinkedList<SQLQuery>();
		//construct
		
//		Utils.checkTrue(aggrExprs.size() == 1); //FIXME
		List<Integer> keyList = new LinkedList<Integer>();
		List<List<AggregateExpr>> exprList = new LinkedList<List<AggregateExpr>>();
		for(Integer key : aggrExprs.keySet()) {
			keyList.add(key);
			exprList.add(aggrExprs.get(key));
			Log.logln("key: " + key + ",  exprList: " + exprList);
		}
		
		List<List<AggregateExpr>> allCombinations = Collections.emptyList();
		if(!exprList.isEmpty()) {
			allCombinations = Maths.allCombination(exprList);
		}
		
		if(allCombinations.isEmpty()) {
			SQLQuery q = new SQLQuery(skeleton);
			q.setGroupbyColumns(groupbyColumns);
			queries.add(q);
			return queries;
		}
		
		for(List<AggregateExpr> list : allCombinations) {
			Utils.checkTrue(list.size() == keyList.size());
			Map<Integer, AggregateExpr> map = new LinkedHashMap<Integer, AggregateExpr>();
			for(int i = 0; i < keyList.size(); i++) {
				map.put(keyList.get(i), list.get(i));
			}
			SQLQuery q = new SQLQuery(skeleton);
			q.setAggregateExprs(map);
			Log.logln(map.toString());
			q.setGroupbyColumns(groupbyColumns);
			queries.add(q);
			Log.logln("sql: " + q.toSQLString());
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