package edu.washington.cs.sqlsynth.entity;

import edu.washington.cs.sqlsynth.entity.ConditionNode.OP;
import edu.washington.cs.sqlsynth.entity.QueryCondition.CONJ;
import edu.washington.cs.sqlsynth.entity.TableColumn.ColumnType;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestQueryCondition extends TestCase {
	
	public static Test suite() {
		return new TestSuite(TestQueryCondition.class);
	}

	public void testConstructConditionNode() {
		TableColumn c1 = new TableColumn("table1", "column1", ColumnType.Integer, false);
		TableColumn c2 = new TableColumn("table2", "column2", ColumnType.Integer, false);
		//, TableColumn rightColumn, Object rightConstant
		ConditionNode leaf1 = new ConditionNode(OP.GT, c1, c2, null); //predicate: table1.column1 > table2.column2
		ConditionNode leaf2 = new ConditionNode(OP.GT, c2, null, 10); //predicate: table2.column2 > 10
		
		//construct a compound condition node: (table1.column1 > table2.column2) || (table2.column2 > 10)
		ConditionNode root = new ConditionNode(CONJ.OR, leaf1, leaf2);
		
		//create a query condition object
		QueryCondition cond = new QueryCondition(root);
		String sql = cond.toSQLCode();
		System.out.println(sql);
		
		assertEquals("(table1.column1 > table2.column2) or (table2.column2 > 10)", sql);
	}
	
	//a slightly more complex example
	//construct a compound predicate:
	//    t1.c1 = t2.c2 && (t3.c3 = 20 or t4.c4 = t2.c2)
	public void testConstructConditionNode1() {
		TableColumn c1 = new TableColumn("t1", "c1", ColumnType.String, false);
		TableColumn c2 = new TableColumn("t2", "c2", ColumnType.String, false);
		TableColumn c3 = new TableColumn("t3", "c3", ColumnType.Integer, false);
		TableColumn c4 = new TableColumn("tr", "c4", ColumnType.String, false);
		
		ConditionNode leaf1 = new ConditionNode(OP.EQ, c1, c2, null);
		ConditionNode leaf2 = new ConditionNode(OP.EQ, c3, null, 20);
		ConditionNode leaf3 = new ConditionNode(OP.EQ, c4, c2, null);
		
		ConditionNode compound1 = new ConditionNode(CONJ.OR, leaf2, leaf3);
		ConditionNode root = new ConditionNode(CONJ.AND, leaf1, compound1);
		
		//create a query condition
		QueryCondition cond = new QueryCondition(root);
		String sql = cond.toSQLCode();
		System.out.println(sql);
		
		assertEquals("(t1.c1 = t2.c2) and ((t3.c3 = 20) or (tr.c4 = t2.c2))", sql);
	}
}
