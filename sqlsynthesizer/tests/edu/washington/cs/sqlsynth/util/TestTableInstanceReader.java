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
		showTables("./dat/id_name", true);
		showTables("./dat/id_name_salary", true);
		showTables("./dat/id_salary", true);
	}
	
	public void testIDNameSalary_NoSchema() {
		showTables("./dat/id_name", false);
		showTables("./dat/id_name_salary", false);
		showTables("./dat/id_salary", false);
	}
	
	public void showTables(String fileName, boolean withSchema) {
		TableInstanceReader reader = new TableInstanceReader(fileName);
		reader.setSchema(withSchema);
		TableInstance table = reader.getTableInstance();
		System.out.println(table);
		
		for(TableColumn column : table.getColumns()) {
			System.out.println(column);
			System.out.println();
		}
	}
	
}
