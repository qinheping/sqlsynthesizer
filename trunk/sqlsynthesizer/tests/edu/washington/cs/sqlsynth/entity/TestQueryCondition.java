package edu.washington.cs.sqlsynth.entity;

import java.util.HashMap;
import java.util.Map;

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
	
	//Room != R128
	public void testParseCondNode1() {
		String cond = "Room != R128";
		Map<String, TableColumn> columnMap = new HashMap<String, TableColumn>();
		columnMap.put("Room", new TableColumn("tbl", "Room", ColumnType.String, false));
		ConditionNode node = QueryCondition.parseNode(columnMap, cond);
		System.out.println(node.toSQLString());
		assertEquals("tbl.Room != R128", node.toSQLString());
		
		ConditionNode revNode = ConditionNode.reverseOp(node);
		System.out.println(revNode.toSQLString());
		assertEquals("tbl.Room = R128", revNode.toSQLString());
	}
	
	//ID_key_ID_key_student_count <= 4.0
	public void testParseCondNode2() {
		String cond = "ID_key_ID_key_student_count <= 4.0";
		Map<String, TableColumn> columnMap = new HashMap<String, TableColumn>();
		columnMap.put("ID_key_ID_key_student_count", new TableColumn("tbl", "ID_key_ID_key_student_count", ColumnType.Integer, false));
		ConditionNode node = QueryCondition.parseNode(columnMap, cond);
		System.out.println(node.toSQLString());
		assertEquals("tbl.ID_key_ID_key_student_count <= 4", node.toSQLString());
		
		ConditionNode revNode = ConditionNode.reverseOp(node);
		System.out.println(revNode.toSQLString());
		assertEquals("tbl.ID_key_ID_key_student_count > 4", revNode.toSQLString());
	}
	
	//ID_key_ID_key_student_count <= 4.0 AND Room != R128
	public void testParseQueryCondition() {
		String cond = "ID_key_ID_key_student_count <= 4.0 AND Room != R128";
		Map<String, TableColumn> columnMap = new HashMap<String, TableColumn>();
		columnMap.put("ID_key_ID_key_student_count", new TableColumn("tbl", "ID_key_ID_key_student_count", ColumnType.Integer, false));
		columnMap.put("Room", new TableColumn("tbl", "Room", ColumnType.String, false));
		
		QueryCondition queryCond = QueryCondition.parse(columnMap, cond);
		System.out.println(queryCond.toSQLCode());
		assertEquals("(tbl.ID_key_ID_key_student_count <= 4 and tbl.Room != R128)", queryCond.toSQLCode());
		
		//test reverse query
		QueryCondition reverseQ = QueryCondition.reverse(queryCond);
		System.out.println(reverseQ.toSQLCode());
		//NOT (ID_key_ID_key_student_count <= 4.0 AND Room != R128)
		assertEquals("(tbl.ID_key_ID_key_student_count > 4 or tbl.Room = R128)", reverseQ.toSQLCode());
	}
	
	//Test transforming NOT statements
	//NOT (ID_key_ID_key_student_count <= 4.0 AND Room != R128) - see above
	//(NOT (ID_key_room_count > 1.0)) AND (room = R128) - see testParseNotQuery2
	//((NOT (C1)) AND (NOT (C2))) AND (NOT (C3))
	//((NOT (C1)) AND (NOT (C2))) AND (C3)
	public void testParseNotQuery() {
		Map<String, TableColumn> columnMap = new HashMap<String, TableColumn>();
		String cond = "(NOT (ID_key_room_count > 1.0)) AND (room = R128)";
		
		columnMap.put("ID_key_room_count", new TableColumn("tbl", "ID_key_room_count", ColumnType.Integer, false));
		columnMap.put("room", new TableColumn("tbl", "room", ColumnType.String, false));
		
		QueryCondition queryCond = QueryCondition.parse(columnMap, cond);
		System.out.println(queryCond.toSQLCode());
		assertEquals("(tbl.ID_key_room_count <= 1 and tbl.room = R128)", queryCond.toSQLCode());
	
	}
	
	public void testParseNotQuery2() {
		Map<String, TableColumn> columnMap = new HashMap<String, TableColumn>();
		String cond = "NOT (ID_key_room_count > 1.0 AND room = R128)";
		
		columnMap.put("ID_key_room_count", new TableColumn("tbl", "ID_key_room_count", ColumnType.Integer, false));
		columnMap.put("room", new TableColumn("tbl", "room", ColumnType.String, false));
		
		QueryCondition queryCond = QueryCondition.parse(columnMap, cond);
		System.out.println(queryCond.toSQLCode());
		assertEquals("(tbl.ID_key_room_count <= 1 or tbl.room != R128)", queryCond.toSQLCode());
		
	}
	
	public void testParseNotQuery3() {
		Map<String, TableColumn> columnMap = new HashMap<String, TableColumn>();
		String cond = "NOT (ID_key_room_count > 1.0 AND NOT (room = R128))";
		
		columnMap.put("ID_key_room_count", new TableColumn("tbl", "ID_key_room_count", ColumnType.Integer, false));
		columnMap.put("room", new TableColumn("tbl", "room", ColumnType.String, false));
		
		QueryCondition queryCond = QueryCondition.parse(columnMap, cond);
		System.out.println(queryCond.toSQLCode());
		assertEquals("(tbl.ID_key_room_count <= 1 or tbl.room = R128)", queryCond.toSQLCode());
		
	}
	
	public void testParseNested() {
		Map<String, TableColumn> columnMap = new HashMap<String, TableColumn>();
		//((NOT (C1)) AND (NOT (C2))) AND (NOT (C3))
		String cond = "((NOT (ID_key_room_count > 1.0)) AND (NOT (ID_key_room_count <= 2.0))) AND (NOT (room = R128))";
		
		columnMap.put("ID_key_room_count", new TableColumn("tbl", "ID_key_room_count", ColumnType.Integer, false));
		columnMap.put("room", new TableColumn("tbl", "room", ColumnType.String, false));
		
		QueryCondition queryCond = QueryCondition.parse(columnMap, cond);
		System.out.println(queryCond.toSQLCode());
		assertEquals("((tbl.ID_key_room_count <= 1 and tbl.ID_key_room_count > 2) and tbl.room != R128)", queryCond.toSQLCode());
	}
	
	public void testParseNested2() {
		Map<String, TableColumn> columnMap = new HashMap<String, TableColumn>();
		//((NOT (C1)) AND (NOT (C2))) AND (C3)
		String cond = "((NOT (ID_key_room_count > 1.0)) AND (NOT (ID_key_room_count <= 2.0))) AND ((room = R128))";
		
		columnMap.put("ID_key_room_count", new TableColumn("tbl", "ID_key_room_count", ColumnType.Integer, false));
		columnMap.put("room", new TableColumn("tbl", "room", ColumnType.String, false));
		
		QueryCondition queryCond = QueryCondition.parse(columnMap, cond);
		System.out.println(queryCond.toSQLCode());
		assertEquals("((tbl.ID_key_room_count <= 1 and tbl.ID_key_room_count > 2) and tbl.room = R128)", queryCond.toSQLCode());
	}
	
	public void testSplitAtTop() {
		String AND = "AND";
		String cond = "((NOT (ID_key_room_count > 1.0)) AND (NOT (ID_key_room_count <= 2.0))) AND (NOT (room = R128))";
		String[] strs = QueryCondition.splitAtTopLevel(cond, AND);
		for(String str : strs) {
			//((NOT (ID_key_room_count > 1.0)) AND((NOT (ID_key_room_count > 1.0))
			//((NOT (ID_key_room_count > 1.0)) AND (NOT (ID_key_room_count <= 2.0)))
			System.out.println(str);
		}
		assertEquals(2, strs.length);
		
		cond = "((NOT (ID_key_room_count > 1.0)) AND (NOT (ID_key_room_count <= 2.0)))";
		cond = QueryCondition.eliminateMatchedPara(cond);
		strs = QueryCondition.splitAtTopLevel(cond, AND);
		for(String str : strs) {
			//((NOT (ID_key_room_count > 1.0)) AND((NOT (ID_key_room_count > 1.0))
			//((NOT (ID_key_room_count > 1.0)) AND (NOT (ID_key_room_count <= 2.0)))
			System.out.println(str);
		}
	}
	
}
