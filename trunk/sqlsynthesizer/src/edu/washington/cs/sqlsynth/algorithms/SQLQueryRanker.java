package edu.washington.cs.sqlsynth.algorithms;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.washington.cs.sqlsynth.entity.NotExistStmt;
import edu.washington.cs.sqlsynth.entity.QueryCondition;
import edu.washington.cs.sqlsynth.entity.SQLQuery;
import edu.washington.cs.sqlsynth.util.Utils;

public class SQLQueryRanker {

	public static List<SQLQuery> rankSQLQueries(Collection<SQLQuery> inputQueries) {
		Map<SQLQuery, Double> scoreMap = new LinkedHashMap<SQLQuery, Double>();
		for(SQLQuery q : inputQueries) {
			double score = calculateSQLQueryCost(q);
			System.out.println(score);
			scoreMap.put(q, score);
		}
		scoreMap = Utils.sortByValue(scoreMap, true);
		List<SQLQuery> rankedQueries = new LinkedList<SQLQuery>();
		rankedQueries.addAll(scoreMap.keySet());
		return rankedQueries;
	}
	
	//simply count the number of each element
	public static double calculateSQLQueryCost (SQLQuery query) {
		int queryLength = query.toSQLString().length();
		
//		int numOfTables = query.getSkeleton().getTables().size();
//		
//		QueryCondition queryCondition = query.getCondition();
//		QueryCondition havingCondition = query.getHavingCond();
//		NotExistStmt notExitStmt = query.getNotExistStmt();
		
		return (double)queryLength;
	}
}