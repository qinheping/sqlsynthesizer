package edu.washington.cs.sqlsynth.util;

import edu.washington.cs.sqlsynth.entity.TableColumn;
import edu.washington.cs.sqlsynth.entity.TableInstance;
import junit.framework.TestCase;

public class TestTableInstanceReader extends TestCase {

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
