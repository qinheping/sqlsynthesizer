package edu.washington.cs.sqlsynth.util;

import edu.washington.cs.sqlsynth.entity.TableColumn;
import edu.washington.cs.sqlsynth.entity.TableInstance;
import junit.framework.TestCase;

public class TestTableInstanceReader extends TestCase {

	public void testParseSimpleFile() {
		TableInstanceReader reader = new TableInstanceReader("./dat/id_name_salary");
		TableInstance table = reader.getTableInstance();
		System.out.println(table);
		
		for(TableColumn column : table.getColumns()) {
			System.out.println(column);
			System.out.println();
		}
	}
	
}
