package edu.washington.cs.sqlsynth.algorithms;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.washington.cs.sqlsynth.db.DbConnector;
import edu.washington.cs.sqlsynth.entity.AggregateExpr;
import edu.washington.cs.sqlsynth.entity.SQLQuery;
import edu.washington.cs.sqlsynth.entity.SQLSkeleton;
import edu.washington.cs.sqlsynth.entity.TableColumn;
import edu.washington.cs.sqlsynth.entity.TableInstance;
import edu.washington.cs.sqlsynth.util.TableInstanceReader;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestSQLCompletor extends TestCase {
	
	public static Test suite() {
		return new TestSuite(TestSQLCompletor.class);
	}

	public void testDTree1()
	{
		TableInstance input1 = TableInstanceReader.readTableFromFile("./dat/5_1_3/id_class_5_1_3");
		TableInstance input2 = TableInstanceReader.readTableFromFile("./dat/5_1_3/id_enroll_5_1_3");
		TableInstance output = TableInstanceReader.readTableFromFile("./dat/5_1_3/output_5_1_3");
		
		Collection<TableInstance> inputs = new LinkedList<TableInstance>();
		inputs.add(input1);
		inputs.add(input2);
		SQLSkeletonCreator creator = new SQLSkeletonCreator(inputs, output);
		SQLSkeleton skeleton = creator.inferSQLSkeleton();
		
		System.out.println("input 1:");
		System.out.println(input1);
		System.out.println("input 2:");
		System.out.println(input2);
		
		System.out.println("number of join columns: " + skeleton.getJoinPairNum());
		
		SQLQueryCompletor completor = new SQLQueryCompletor(skeleton);
		completor.addInputTable(input1);
		completor.addInputTable(input2);
		completor.setOutputTable(output);
		
		List<SQLQuery> queries = completor.inferSQLQueries();
		System.out.println("number of inferred queries: " + queries.size());
		for(SQLQuery q : queries) {
			System.out.println(q.toSQLString());
		}
		System.out.println();
		queries = completor.validateQueriesOnDb(queries);
		//after validating on my sql
		System.out.println("The final output....");
		for(SQLQuery q : queries) {
			System.out.println(q.toSQLString());
		}
	}
	
	public void testDTree2()
	{
		TableInstance input1 = TableInstanceReader.readTableFromFile("./dat/5_1_5/id_class_5_1_5");
		TableInstance input2 = TableInstanceReader.readTableFromFile("./dat/5_1_5/id_faculty_5_1_5");
		TableInstance output = TableInstanceReader.readTableFromFile("./dat/5_1_5/output_5_1_5");
		
		Collection<TableInstance> inputs = new LinkedList<TableInstance>();
		inputs.add(input1);
		inputs.add(input2);
		SQLSkeletonCreator creator = new SQLSkeletonCreator(inputs, output);
		SQLSkeleton skeleton = creator.inferSQLSkeleton();
		
		System.out.println("input 1:");
		System.out.println(input1);
		System.out.println("input 2:");
		System.out.println(input2);
		
		System.out.println("number of join columns: " + skeleton.getJoinPairNum());
		
		SQLQueryCompletor completor = new SQLQueryCompletor(skeleton);
		completor.addInputTable(input1);
		completor.addInputTable(input2);
		completor.setOutputTable(output);
		
		List<SQLQuery> queries = completor.inferSQLQueries();
		for(SQLQuery q : queries) {
			System.out.println(q.toSQLString());
		}
		queries = completor.validateQueriesOnDb(queries);
		//after validating on my sql
		System.out.println("The final output....");
		for(SQLQuery q : queries) {
			System.out.println(q.toSQLString());
		}
	}
	
	
	
	public void testDTree3()
	{
		TableInstance input1 = TableInstanceReader.readTableFromFile("./dat/5_1_9/id_class_5_1_9");
		TableInstance input2 = TableInstanceReader.readTableFromFile("./dat/5_1_9/id_faculty_5_1_9");
		TableInstance output = TableInstanceReader.readTableFromFile("./dat/5_1_9/output_5_1_9");
		
		Collection<TableInstance> inputs = new LinkedList<TableInstance>();
		inputs.add(input1);
		inputs.add(input2);
		SQLSkeletonCreator creator = new SQLSkeletonCreator(inputs, output);
		SQLSkeleton skeleton = creator.inferSQLSkeleton();
		
		System.out.println("input 1:");
		System.out.println(input1);
		System.out.println("input 2:");
		System.out.println(input2);
		
		System.out.println("number of join columns: " + skeleton.getJoinPairNum());
		
		SQLQueryCompletor completor = new SQLQueryCompletor(skeleton);
		completor.addInputTable(input1);
		completor.addInputTable(input2);
		completor.setOutputTable(output);
		
		List<SQLQuery> queries = completor.inferSQLQueries();
		for(SQLQuery q : queries) {
			System.out.println(q.toSQLString());
		}
		queries = completor.validateQueriesOnDb(queries);
		//after validating on my sql
		System.out.println("The final output....");
		for(SQLQuery q : queries) {
			System.out.println(q.toSQLString());
		}
	}
	
	
	public void testDTree4()
	{
		TableInstance input1 = TableInstanceReader.readTableFromFile("./dat/5_1_10/student");
		TableInstance input2 = TableInstanceReader.readTableFromFile("./dat/5_1_10/enrolled");
		TableInstance output = TableInstanceReader.readTableFromFile("./dat/5_1_10/output");
		
		Collection<TableInstance> inputs = new LinkedList<TableInstance>();
		inputs.add(input1);
		inputs.add(input2);
		SQLSkeletonCreator creator = new SQLSkeletonCreator(inputs, output);
		SQLSkeleton skeleton = creator.inferSQLSkeleton();
		
		System.out.println("input 1:");
		System.out.println(input1);
		System.out.println("input 2:");
		System.out.println(input2);
		
		System.out.println("number of join columns: " + skeleton.getJoinPairNum());
		
		SQLQueryCompletor completor = new SQLQueryCompletor(skeleton);
		completor.addInputTable(input1);
		completor.addInputTable(input2);
		completor.setOutputTable(output);
		
		List<SQLQuery> queries = completor.inferSQLQueries();
		for(SQLQuery q : queries) {
			System.out.println(q.toSQLString());
		}
		queries = completor.validateQueriesOnDb(queries);
		//after validating on my sql
		System.out.println("The final output....");
		for(SQLQuery q : queries) {
			System.out.println(q.toSQLString());
		}
	}
	
	
	public void test1() {
		TableInstance input1 = TableInstanceReader.readTableFromFile("./dat/id_name");
		TableInstance input2 = TableInstanceReader.readTableFromFile("./dat/id_salary");
		TableInstance output = TableInstanceReader.readTableFromFile("./dat/id_name_salary_full");
		
		Collection<TableInstance> inputs = new LinkedList<TableInstance>();
		inputs.add(input1);
		inputs.add(input2);
		SQLSkeletonCreator creator = new SQLSkeletonCreator(inputs, output);
		SQLSkeleton skeleton = creator.inferSQLSkeleton();
		
		SQLQueryCompletor completor = new SQLQueryCompletor(skeleton);
		completor.addInputTable(input1);
		completor.addInputTable(input2);
		completor.setOutputTable(output);
		
		List<SQLQuery> queries = completor.inferSQLQueries();
		for(SQLQuery q : queries) {
			System.out.println(q.toSQLString());
		}
		queries = completor.validateQueriesOnDb(queries);
		//after validating on my sql
		System.out.println("The final output....");
		for(SQLQuery q : queries) {
			System.out.println(q.toSQLString());
		}
	}
	
	public void test2() {
		TableInstance input = TableInstanceReader.readTableFromFile("./dat/groupby/name_salary");
		TableInstance output = TableInstanceReader.readTableFromFile("./dat/groupby/name_salary_count");
		
		Collection<TableInstance> inputs = new LinkedList<TableInstance>();
		inputs.add(input);
		SQLSkeletonCreator creator = new SQLSkeletonCreator(inputs, output);
		SQLSkeleton skeleton = creator.inferSQLSkeleton();
		
		SQLQueryCompletor completor = new SQLQueryCompletor(skeleton);
		completor.addInputTable(input);
		completor.setOutputTable(output);
		
		AggregateExprInfer aggInfer = new AggregateExprInfer(completor);
		Map<Integer, List<AggregateExpr>> aggrExprs = aggInfer.inferAggregationExprs();
		List<TableColumn> groupbyColumns = aggInfer.inferGroupbyColumns();
		
		//create SQL statements
		
		List<SQLQuery> queries = new LinkedList<SQLQuery>();
		queries.addAll(completor.constructQueries(skeleton, aggrExprs, groupbyColumns));
		
		for(SQLQuery q : queries) {
			System.out.println(q.toSQLString());
		}
		DbConnector.NO_ORDER_MATCHING = true;
		queries = completor.validateQueriesOnDb(queries);
		//after validating on my sql
		System.out.println("The final output....");
		for(SQLQuery q : queries) {
			System.out.println(q.toSQLString());
		}
	}
	
	public void testDull() {
		for(int i = 0; i<5; ++i) {
			System.out.println(i);
		}
	}
	
	@Override
	public void tearDown() {
		DbConnector.NO_ORDER_MATCHING = false;
	}
	
}
