package edu.washington.cs.sqlsynth.algorithms;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.washington.cs.sqlsynth.db.DbConnector;
import edu.washington.cs.sqlsynth.entity.AggregateExpr;
import edu.washington.cs.sqlsynth.entity.NotExistStmt;
import edu.washington.cs.sqlsynth.entity.SQLQuery;
import edu.washington.cs.sqlsynth.entity.SQLSkeleton;
import edu.washington.cs.sqlsynth.entity.TableColumn;
import edu.washington.cs.sqlsynth.entity.TableInstance;
import edu.washington.cs.sqlsynth.util.TableInstanceReader;
import edu.washington.cs.sqlsynth.util.TableUtils;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestSQLCompletor extends TestCase {
	
	public static Test suite() {
		return new TestSuite(TestSQLCompletor.class);
	}
	
	public void test5_1_1()
	{	
		TableUtils.USE_SAME_NAME_JOIN = true;
		TableUtils.JOIN_ALL_TABLES = true;
		DbConnector.NO_ORDER_MATCHING = true;
		
		TableInstance input1 = TableInstanceReader.readTableFromFile("./dat/5_1_1/class");
		TableInstance input2 = TableInstanceReader.readTableFromFile("./dat/5_1_1/enroll");
		TableInstance input3 = TableInstanceReader.readTableFromFile("./dat/5_1_1/student");
		TableInstance input4 = TableInstanceReader.readTableFromFile("./dat/5_1_1/faculty");
		TableInstance output = TableInstanceReader.readTableFromFile("./dat/5_1_1/output");
		
		Collection<TableInstance> inputs = new LinkedList<TableInstance>();
		inputs.add(input1);
		inputs.add(input2);
		inputs.add(input3);
		inputs.add(input4);
		SQLSkeletonCreator creator = new SQLSkeletonCreator(inputs, output);
		SQLSkeleton skeleton = creator.inferSQLSkeleton();
		
		List<TableInstance> tables = skeleton.computeJoinTableWithoutUnmatches();
		
		for(TableInstance t : tables) {
			System.out.println(t.toString());
		}
		System.out.println("Number of tables: " + tables.size());
		System.out.println("The skeleton: ");
		System.out.println(skeleton);
		
//		if(tables != null) {
//			return;
//		}
		
		System.out.println("input 1:");
		System.out.println(input1);
		System.out.println("input 2:");
		System.out.println(input2);
		System.out.println("input 3:");
		System.out.println(input3);
		System.out.println("input 4:");
		System.out.println(input4);
		
		
		System.out.println("number of join columns: " + skeleton.getJoinPairNum());
		
		SQLQueryCompletor completor = new SQLQueryCompletor(skeleton);
		completor.addInputTable(input1);
		completor.addInputTable(input2);
		completor.addInputTable(input3);
		completor.addInputTable(input4);
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
	
	
	public void test5_1_2()
	{	
		TableUtils.USE_SAME_NAME_JOIN = true;
		TableUtils.JOIN_ALL_TABLES = true;
		DbConnector.NO_ORDER_MATCHING = true;
		
		TableInstance input1 = TableInstanceReader.readTableFromFile("./dat/5_1_2/class");
		TableInstance input2 = TableInstanceReader.readTableFromFile("./dat/5_1_2/enroll");
		TableInstance input3 = TableInstanceReader.readTableFromFile("./dat/5_1_2/student");
		TableInstance input4 = TableInstanceReader.readTableFromFile("./dat/5_1_2/faculty");
		TableInstance output = TableInstanceReader.readTableFromFile("./dat/5_1_2/output");
		
		Collection<TableInstance> inputs = new LinkedList<TableInstance>();
		inputs.add(input1);
		inputs.add(input2);
		inputs.add(input3);
		inputs.add(input4);
		SQLSkeletonCreator creator = new SQLSkeletonCreator(inputs, output);
		SQLSkeleton skeleton = creator.inferSQLSkeleton();
		
		List<TableInstance> tables = skeleton.computeJoinTableWithoutUnmatches();
		
		for(TableInstance t : tables) {
			System.out.println(t.toString());
		}
		System.out.println("Number of tables: " + tables.size());
		
//		if(tables != null) {
//			return;
//		}
		
		System.out.println("input 1:");
		System.out.println(input1);
		System.out.println("input 2:");
		System.out.println(input2);
		System.out.println("input 3:");
		System.out.println(input3);
		System.out.println("input 4:");
		System.out.println(input4);
		
		
		System.out.println("number of join columns: " + skeleton.getJoinPairNum());
		
		SQLQueryCompletor completor = new SQLQueryCompletor(skeleton);
		completor.addInputTable(input1);
		completor.addInputTable(input2);
		completor.addInputTable(input3);
		completor.addInputTable(input4);
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
	
	
	
	public void test5_1_4()
	{	
		TableUtils.USE_SAME_NAME_JOIN = true;
		TableUtils.JOIN_ALL_TABLES = true;
		DbConnector.NO_ORDER_MATCHING = true;
		
		TableInstance input1 = TableInstanceReader.readTableFromFile("./dat/5_1_4/class");
		TableInstance input2 = TableInstanceReader.readTableFromFile("./dat/5_1_4/enroll");
		TableInstance input3 = TableInstanceReader.readTableFromFile("./dat/5_1_4/student");

		TableInstance output = TableInstanceReader.readTableFromFile("./dat/5_1_4/output");
		
		Collection<TableInstance> inputs = new LinkedList<TableInstance>();
		inputs.add(input1);
		inputs.add(input2);
		inputs.add(input3);
		SQLSkeletonCreator creator = new SQLSkeletonCreator(inputs, output);
		SQLSkeleton skeleton = creator.inferSQLSkeleton();
		
		List<TableInstance> tables = skeleton.computeJoinTableWithoutUnmatches();
		
		for(TableInstance t : tables) {
			System.out.println(t.toString());
		}
		System.out.println("Number of tables: " + tables.size());
		
//		if(tables != null) {
//			return;
//		}
		
		System.out.println("input 1:");
		System.out.println(input1);
		System.out.println("input 2:");
		System.out.println(input2);
		System.out.println("input 3:");
		System.out.println(input3);
		
		System.out.println("number of join columns: " + skeleton.getJoinPairNum());
		
		SQLQueryCompletor completor = new SQLQueryCompletor(skeleton);
		completor.addInputTable(input1);
		completor.addInputTable(input2);
		completor.addInputTable(input3);
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
	
	
	
	public void test5_1_6()
	{	
		TableUtils.USE_SAME_NAME_JOIN = true;
		TableUtils.JOIN_ALL_TABLES = true;
		DbConnector.NO_ORDER_MATCHING = true;
		
		TableInstance input1 = TableInstanceReader.readTableFromFile("./dat/5_1_6/class");
		TableInstance input2 = TableInstanceReader.readTableFromFile("./dat/5_1_6/enroll");
		TableInstance input3 = TableInstanceReader.readTableFromFile("./dat/5_1_6/faculty");

		TableInstance output = TableInstanceReader.readTableFromFile("./dat/5_1_6/output");
		
		Collection<TableInstance> inputs = new LinkedList<TableInstance>();
		inputs.add(input1);
		inputs.add(input2);
		inputs.add(input3);
		SQLSkeletonCreator creator = new SQLSkeletonCreator(inputs, output);
		SQLSkeleton skeleton = creator.inferSQLSkeleton();
		
		List<TableInstance> tables = skeleton.computeJoinTableWithoutUnmatches();
		
		for(TableInstance t : tables) {
			System.out.println(t.toString());
		}
		System.out.println("Number of tables: " + tables.size());
		
//		if(tables != null) {
//			return;
//		}
		
		System.out.println("input 1:");
		System.out.println(input1);
		System.out.println("input 2:");
		System.out.println(input2);
		System.out.println("input 3:");
		System.out.println(input3);
		
		System.out.println("number of join columns: " + skeleton.getJoinPairNum());
		
		SQLQueryCompletor completor = new SQLQueryCompletor(skeleton);
		completor.addInputTable(input1);
		completor.addInputTable(input2);
		completor.addInputTable(input3);
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
	
	

	public void test5_1_3()
	{	
		DbConnector.NO_ORDER_MATCHING = true;
		
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
	
	public void test5_1_5()
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
	
	//NOTE this can not be figured by our language subset
	public void test5_1_9()
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
	
	
	public void test5_1_10()
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
	
	
	public void testDTree5()
	{
		TableInstance input1 = TableInstanceReader.readTableFromFile("./dat/single_table_1/provide_relation");
		TableInstance output = TableInstanceReader.readTableFromFile("./dat/single_table_1/output");
		
		Collection<TableInstance> inputs = new LinkedList<TableInstance>();
		inputs.add(input1);
		SQLSkeletonCreator creator = new SQLSkeletonCreator(inputs, output);
		SQLSkeleton skeleton = creator.inferSQLSkeleton();
		
		System.out.println("input 1:");
		System.out.println(input1);
		
		System.out.println("number of join columns: " + skeleton.getJoinPairNum());
		
		SQLQueryCompletor completor = new SQLQueryCompletor(skeleton);
		completor.addInputTable(input1);
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
	
	public void testDTree6()
	{
		TableInstance input1 = TableInstanceReader.readTableFromFile("./dat/single_table_2/tbl1");
		TableInstance output = TableInstanceReader.readTableFromFile("./dat/single_table_2/output");
		
		Collection<TableInstance> inputs = new LinkedList<TableInstance>();
		inputs.add(input1);
		SQLSkeletonCreator creator = new SQLSkeletonCreator(inputs, output);
		SQLSkeleton skeleton = creator.inferSQLSkeleton();
		
		System.out.println("input 1:");
		System.out.println(input1);
		
		System.out.println("number of join columns: " + skeleton.getJoinPairNum());
		
		SQLQueryCompletor completor = new SQLQueryCompletor(skeleton);
		completor.addInputTable(input1);
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
	
	public void testDTree7()
	{
		TableInstance input1 = TableInstanceReader.readTableFromFile("./dat/two_tables_order_by/vote");
		TableInstance output = TableInstanceReader.readTableFromFile("./dat/two_tables_order_by/output");
		
		Collection<TableInstance> inputs = new LinkedList<TableInstance>();
		inputs.add(input1);
		SQLSkeletonCreator creator = new SQLSkeletonCreator(inputs, output);
		SQLSkeleton skeleton = creator.inferSQLSkeleton();
		
		System.out.println("input 1:");
		System.out.println(input1);
		
		System.out.println("number of join columns: " + skeleton.getJoinPairNum());
		
		SQLQueryCompletor completor = new SQLQueryCompletor(skeleton);
		completor.addInputTable(input1);
		completor.setOutputTable(output);
		
		DbConnector.NO_ORDER_MATCHING = true;
		
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
	
	public void testDTree8()
	{
		TableInstance input1 = TableInstanceReader.readTableFromFile("./dat/four_tables/house");
		TableInstance input2 = TableInstanceReader.readTableFromFile("./dat/four_tables/property");
		TableInstance input3 = TableInstanceReader.readTableFromFile("./dat/four_tables/person");
		TableInstance input4 = TableInstanceReader.readTableFromFile("./dat/four_tables/car");
		
		TableInstance output = TableInstanceReader.readTableFromFile("./dat/four_tables/output");
		
		Collection<TableInstance> inputs = new LinkedList<TableInstance>();
		inputs.add(input1);
		inputs.add(input2);
		inputs.add(input3);
		inputs.add(input4);
		
		SQLSkeleton.REMOVE_DUPLICATE = true;
		
		SQLSkeletonCreator creator = new SQLSkeletonCreator(inputs, output);
		SQLSkeleton skeleton = creator.inferSQLSkeleton();
		
		System.out.println("input 1:");
		System.out.println(input1);
		System.out.println("input 2:");
		System.out.println(input2);
		System.out.println("input 3:");
		System.out.println(input3);
		System.out.println("input 4:");
		System.out.println(input4);
		
		System.out.println("number of join columns: " + skeleton.getJoinPairNum());
		
		SQLQueryCompletor completor = new SQLQueryCompletor(skeleton);
		completor.addInputTable(input1);
		completor.addInputTable(input2);
		completor.addInputTable(input3);
		completor.addInputTable(input4);
		
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
	
	
	public void test5_2_2()
	{
		TableInstance input1 = TableInstanceReader.readTableFromFile("./dat/5_2_2/parts");
		TableInstance input2 = TableInstanceReader.readTableFromFile("./dat/5_2_2/catalog");
		TableInstance input3 = TableInstanceReader.readTableFromFile("./dat/5_2_2/suppliers");
		TableInstance output = TableInstanceReader.readTableFromFile("./dat/5_2_2/output");
		
		Collection<TableInstance> inputs = new LinkedList<TableInstance>();
		inputs.add(input1);
		inputs.add(input2);
		inputs.add(input3);
		
		SQLSkeletonCreator creator = new SQLSkeletonCreator(inputs, output);
		SQLSkeleton skeleton = creator.inferSQLSkeleton();
		
		System.out.println("input 1:");
		System.out.println(input1);
		System.out.println("input 2:");
		System.out.println(input2);
		System.out.println("input 3:");
		System.out.println(input3);
		
		
		System.out.println("number of join columns: " + skeleton.getJoinPairNum());
		System.out.println("columns: " + skeleton.getAllJoinConditions());
		
		List<TableInstance> tables = skeleton.computeJoinTableWithoutUnmatches();
		
		for(TableInstance t : tables) {
			System.out.println(t.toString());
		}
		System.out.println("Number of tables: " + tables.size());
//		
//		if(tables != null) {
//			return;
//		}
		
		SQLQueryCompletor completor = new SQLQueryCompletor(skeleton);
		completor.addInputTable(input1);
		completor.addInputTable(input2);
		completor.addInputTable(input3);
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
		
		//throw new Error();
	}
	
	
	public void test5_2_4()
	{
		TableInstance input1 = TableInstanceReader.readTableFromFile("./dat/5_2_4/parts");
		TableInstance input2 = TableInstanceReader.readTableFromFile("./dat/5_2_4/catalog");
		TableInstance input3 = TableInstanceReader.readTableFromFile("./dat/5_2_4/suppliers");
		TableInstance output = TableInstanceReader.readTableFromFile("./dat/5_2_4/output");
		
		Collection<TableInstance> inputs = new LinkedList<TableInstance>();
		inputs.add(input1);
		inputs.add(input2);
		inputs.add(input3);
		
		SQLSkeletonCreator creator = new SQLSkeletonCreator(inputs, output);
		SQLSkeleton skeleton = creator.inferSQLSkeleton();
		
		System.out.println("input 1:");
		System.out.println(input1);
		System.out.println("input 2:");
		System.out.println(input2);
		System.out.println("input 3:");
		System.out.println(input3);
		
		
		System.out.println("number of join columns: " + skeleton.getJoinPairNum());
		
		SQLQueryCompletor completor = new SQLQueryCompletor(skeleton);
		completor.addInputTable(input1);
		completor.addInputTable(input2);
		completor.addInputTable(input3);
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
	
	
	public void test5_2_7()
	{
		TableInstance input1 = TableInstanceReader.readTableFromFile("./dat/5_2_7/parts");
		TableInstance input2 = TableInstanceReader.readTableFromFile("./dat/5_2_7/catalog");
		TableInstance input3 = TableInstanceReader.readTableFromFile("./dat/5_2_7/suppliers");
		TableInstance output = TableInstanceReader.readTableFromFile("./dat/5_2_7/output");
		
		Collection<TableInstance> inputs = new LinkedList<TableInstance>();
		inputs.add(input1);
		inputs.add(input2);
		inputs.add(input3);
		
		SQLSkeletonCreator creator = new SQLSkeletonCreator(inputs, output);
		SQLSkeleton skeleton = creator.inferSQLSkeleton();
		
		System.out.println("input 1:");
		System.out.println(input1);
		System.out.println("input 2:");
		System.out.println(input2);
		System.out.println("input 3:");
		System.out.println(input3);
		
		
		System.out.println("number of join columns: " + skeleton.getJoinPairNum());
		
		SQLQueryCompletor completor = new SQLQueryCompletor(skeleton);
		completor.addInputTable(input1);
		completor.addInputTable(input2);
		completor.addInputTable(input3);
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
	
	
	
	public void test5_2_8()
	{
		TableInstance input1 = TableInstanceReader.readTableFromFile("./dat/5_2_8/parts");
		TableInstance input2 = TableInstanceReader.readTableFromFile("./dat/5_2_8/catalog");
		TableInstance input3 = TableInstanceReader.readTableFromFile("./dat/5_2_8/suppliers");
		TableInstance output = TableInstanceReader.readTableFromFile("./dat/5_2_8/output");
		
		Collection<TableInstance> inputs = new LinkedList<TableInstance>();
		inputs.add(input1);
		inputs.add(input2);
		inputs.add(input3);
		
		SQLSkeletonCreator creator = new SQLSkeletonCreator(inputs, output);
		SQLSkeleton skeleton = creator.inferSQLSkeleton();
		
		System.out.println("input 1:");
		System.out.println(input1);
		System.out.println("input 2:");
		System.out.println(input2);
		System.out.println("input 3:");
		System.out.println(input3);
		
		
		System.out.println("number of join columns: " + skeleton.getJoinPairNum());
		
		SQLQueryCompletor completor = new SQLQueryCompletor(skeleton);
		completor.addInputTable(input1);
		completor.addInputTable(input2);
		completor.addInputTable(input3);
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
	
	
	
	public void testExampleForPresentation()
	{
		TableInstance input1 = TableInstanceReader.readTableFromFile("./dat/example_for_presentation/student");
		TableInstance input2 = TableInstanceReader.readTableFromFile("./dat/example_for_presentation/enroll");
		TableInstance output = TableInstanceReader.readTableFromFile("./dat/example_for_presentation/output");
		
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
		DbConnector.NO_ORDER_MATCHING = true;
		queries = completor.validateQueriesOnDb(queries);
		//after validating on my sql
		//rank it
		queries = SQLQueryRanker.rankSQLQueries(queries);
		System.out.println("The final output....");
		for(SQLQuery q : queries) {
			System.out.println(q.toSQLString());
		}
		
		//add the not-exist stmt, meaningless code below
		for(SQLQuery q : queries) {
			q.setNotExistStmt(new NotExistStmt(SQLQuery.clone(q)));
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
		SQLSkeleton.REMOVE_DUPLICATE = false;
	}
	
}
