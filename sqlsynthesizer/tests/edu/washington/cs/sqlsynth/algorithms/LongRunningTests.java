package edu.washington.cs.sqlsynth.algorithms;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import edu.washington.cs.sqlsynth.db.DbConnector;
import edu.washington.cs.sqlsynth.entity.AggregateExpr;
import edu.washington.cs.sqlsynth.entity.SQLQuery;
import edu.washington.cs.sqlsynth.entity.SQLSkeleton;
import edu.washington.cs.sqlsynth.entity.TableColumn;
import edu.washington.cs.sqlsynth.entity.TableInstance;
import edu.washington.cs.sqlsynth.util.Log;
import edu.washington.cs.sqlsynth.util.TableInstanceReader;

public class LongRunningTests extends TestCase {
	
	//around 2 mins
	public void test5() {
		AggregateExpr.moreStringOp = true;
		DbConnector.NO_ORDER_MATCHING = true;
		DbConnector.NO_DROP_TABLE = true;
		
		Log.logConfig("./log.txt");
		
		long start = System.currentTimeMillis();
		
		TableInstance input1 = TableInstanceReader.readTableFromFile("./dat/proposalexample/table1");
		TableInstance input2 = TableInstanceReader.readTableFromFile("./dat/proposalexample/table2");
		TableInstance input3 = TableInstanceReader.readTableFromFile("./dat/proposalexample/table3");
		TableInstance output = TableInstanceReader.readTableFromFile("./dat/proposalexample/output_nocond");
		
		Collection<TableInstance> inputs = new LinkedList<TableInstance>();
		inputs.add(input1);
		inputs.add(input2);
		inputs.add(input3);
		SQLSkeletonCreator creator = new SQLSkeletonCreator(inputs, output);
		SQLSkeleton skeleton = creator.inferSQLSkeleton();
		
		SQLQueryCompletor completor = new SQLQueryCompletor(skeleton);
		completor.addInputTable(input1);
		completor.addInputTable(input2);
		completor.addInputTable(input3);
		completor.setOutputTable(output);
		
		//create the inferrer
		AggregateExprInfer aggInfer = new AggregateExprInfer(completor);
		Map<Integer, List<AggregateExpr>> aggrExprs = aggInfer.inferAggregationExprs();
		List<TableColumn> groupbyColumns = aggInfer.inferGroupbyColumns();
		
		System.out.println("aggregate expressions:");
		System.out.println(aggrExprs.size());
		System.out.println(aggrExprs);
		System.out.println("group by columns:");
		System.out.println(groupbyColumns);
		System.out.println("all join conditions:");
		System.out.println(skeleton.getAllJoinConditions());
		System.out.println("projection columns:");
		System.out.println(skeleton.getProjectColumns());
		
		List<SQLQuery> queries = completor.constructQueries(skeleton, aggrExprs, groupbyColumns);
		
		System.out.println("Num of queries: " + queries.size());
		List<SQLQuery> filtered = queries; 
		filtered = new LinkedList<SQLQuery>();
		for(SQLQuery sql : queries) {
			//if(sql.toSQLString().trim().startsWith("select min(table1.T1Column1), table2.T2Column3, min(table1.T1Column4), min(table3.T3Column2)")) {
			    System.out.println(sql.toSQLString());
			    filtered.add(sql);
			//}
		}
		
		filtered = completor.validateQueriesOnDb(filtered);
		System.out.println("Num of queries before validation: " + queries.size());
		System.out.println("After validation: size of filtered: " + filtered.size());
		for(SQLQuery sql : filtered) {
			System.out.println(sql.toSQLString());
		}
		
		System.out.println("Time cost: " + (System.currentTimeMillis() - start));
	}
	
	@Override
	public void tearDown() {
		AggregateExpr.moreStringOp = false;
		DbConnector.NO_ORDER_MATCHING = false;
		DbConnector.NO_DROP_TABLE = false;
		Log.removeLog();
	}
}
