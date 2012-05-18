package edu.washington.cs.sqlsynth.entity;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import edu.washington.cs.sqlsynth.util.TableInstanceReader;
import junit.framework.TestCase;

public class TestSQLSkeleton extends TestCase {

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
	
}
