package edu.washington.cs.sqlsynth.entity;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import plume.Pair;

import edu.washington.cs.sqlsynth.db.DbConnector;
import edu.washington.cs.sqlsynth.util.TableInstanceReader;
import edu.washington.cs.sqlsynth.util.Utils;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestSQLSkeleton extends TestCase {

	public static Test suite() {
		return new TestSuite(TestSQLSkeleton.class);
	}
	
	public void testGetMatchedColumns() {
		TableInstance input = TableInstanceReader.readTableFromFile("./dat/groupby/name_salary");
		TableInstance output = TableInstanceReader.readTableFromFile("./dat/groupby/name_salary_count");
		List<TableInstance> inputs = new LinkedList<TableInstance>();
		inputs.add(input);
		SQLSkeleton skeleton = new SQLSkeleton(inputs, output);
		TableInstance inst = skeleton.getTableOnlyWithMatchedColumns();
		System.out.println(inst);
		assertEquals(inst.getColumnNum(), 1);
		assertEquals(inst.getColumns().get(0).getColumnName(), "Name");
	}
	
	public void testGetJoinMatched() {
		TableInstance input1 = TableInstanceReader.readTableFromFile("./dat/id_name");
		TableInstance input2 = TableInstanceReader.readTableFromFile("./dat/id_salary");
		TableInstance output = TableInstanceReader.readTableFromFile("./dat/id_name_salary");
		
		List<TableInstance> tables = new LinkedList<TableInstance>();
		tables.add(input1);
		tables.add(input2);
		
		List<Pair<TableColumn, TableColumn>> joinColumns = new LinkedList<Pair<TableColumn, TableColumn>>();
		TableColumn c1 = input1.getColumnByName("ID_key");
		TableColumn c2 = input2.getColumnByName("ID_key");
		Utils.checkNotNull(c1);
		Utils.checkNotNull(c2);
		Pair<TableColumn, TableColumn> p = new Pair<TableColumn, TableColumn>(c1, c2);
		joinColumns.add(p);
		
		TableInstance t = DbConnector.instance().joinTable(tables, joinColumns);
		System.out.println(t);
		
		SQLSkeleton skeleton = new SQLSkeleton(tables, output);
		skeleton.addJoinColumns(p);
		List<TableInstance> ts = skeleton.computeJoinTableWithoutUnmatches();
		System.out.println(ts);
		
		assertEquals(1, ts.size());
	}
	
}
