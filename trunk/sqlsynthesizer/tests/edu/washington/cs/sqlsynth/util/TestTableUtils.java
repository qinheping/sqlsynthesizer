package edu.washington.cs.sqlsynth.util;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import plume.Pair;

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
}
