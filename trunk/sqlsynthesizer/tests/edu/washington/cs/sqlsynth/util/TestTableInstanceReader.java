package edu.washington.cs.sqlsynth.util;

import edu.washington.cs.sqlsynth.entity.TableColumn;
import edu.washington.cs.sqlsynth.entity.TableInstance;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestTableInstanceReader extends TestCase {
	
	public static Test suite() {
		return new TestSuite(TestTableInstanceReader.class);
	}

	public void testIDNameSalary() {
		showTables("./dat/id_name");
		showTables("./dat/id_name_salary");
		showTables("./dat/id_salary");
	}
	
	public void showTables(String fileName) {
		TableInstanceReader reader = new TableInstanceReader(fileName);
		TableInstance table = reader.getTableInstance();
		System.out.println(table);
		
		for(TableColumn column : table.getColumns()) {
			System.out.println(column);
			System.out.println();
		}
	}
	
}
