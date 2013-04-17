package edu.washington.cs.sqlsynth.util;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import plume.Pair;

import edu.washington.cs.sqlsynth.algorithms.SQLSkeletonCreator;
import edu.washington.cs.sqlsynth.entity.SQLSkeleton;
import edu.washington.cs.sqlsynth.entity.TableColumn;
import edu.washington.cs.sqlsynth.entity.TableInstance;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestTableUtils extends TestCase {
	public static Test suite() {
		return new TestSuite(TestTableUtils.class);
	}
	
	public void testMultiJoin() {
		TableInstance input1 = TableInstanceReader.readTableFromFile("./dat/multijoin/id_name");
		TableInstance input2 = TableInstanceReader.readTableFromFile("./dat/multijoin/id_name_salary");
		Collection<TableInstance> inputs = new LinkedList<TableInstance>();
		inputs.add(input1);
		inputs.add(input2);
		
		Collection<Pair<TableColumn, TableColumn>> joinColumns =
			new LinkedList<Pair<TableColumn, TableColumn>>();
		joinColumns.add(new Pair<TableColumn, TableColumn>(input1.getColumnByName("ID_key"), input2.getColumnByName("ID_key")));
		joinColumns.add(new Pair<TableColumn, TableColumn>(input1.getColumnByName("Name"), input2.getColumnByName("Name")));
		
		List<TableInstance> tables = TableUtils.joinTables(inputs, joinColumns);
		for(TableInstance t : tables) {
			System.out.println(t);
		}
		assertEquals(3, tables.size());
	}
	
	public void testTableSubsume() {
		TableInstance bt = TableInstanceReader.readTableFromFile("./dat/subsume/bigt");
		TableInstance st = TableInstanceReader.readTableFromFile("./dat/subsume/smallt");
		assertTrue(TableUtils.subsume(st, bt));
		assertTrue(!TableUtils.subsume(bt, st));
	}
	
	public void testJoinThreeTables() {
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
		
		List<TableInstance> tables = skeleton.computeJoinTableWithoutUnmatches();
		
		System.out.println("------ the joined tables ---------");
		for(TableInstance t : tables) {
			System.out.println(t.toString());
		}
		
		System.out.println("Number of tables: " + tables.size());
		assertEquals(1, tables.size());
	}
}
