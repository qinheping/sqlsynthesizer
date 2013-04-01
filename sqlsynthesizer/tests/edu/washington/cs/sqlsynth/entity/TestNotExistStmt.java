package edu.washington.cs.sqlsynth.entity;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import edu.washington.cs.sqlsynth.algorithms.SQLQueryCompletor;
import edu.washington.cs.sqlsynth.algorithms.SQLSkeletonCreator;
import edu.washington.cs.sqlsynth.util.TableInstanceReader;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestNotExistStmt extends TestCase {
	
	public static Test suite() {
		return new TestSuite(TestNotExistStmt.class);
	}
	
	/**
	 * Generate a fake one, illegal SQL
	 * */
	public void testSingleExistStmtCase() {
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
			NotExistStmt stmt = new NotExistStmt(q);
			String sql = stmt.toSQLString();
			System.out.println(sql);
			assertTrue(sql.startsWith("NOT EXISTS ("));
		}
	}
	
	/**
	 * A legal SQL generated
	 * */
	public void testCompoundStmtCase() {
		
	}
}
