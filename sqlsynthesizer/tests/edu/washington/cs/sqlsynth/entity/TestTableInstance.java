package edu.washington.cs.sqlsynth.entity;

import edu.washington.cs.sqlsynth.util.Globals;
import edu.washington.cs.sqlsynth.util.TableInstanceReader;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestTableInstance extends TestCase {
	public static Test suite() {
		return new TestSuite(TestTableInstance.class);
	}
	
	/**
	 * Here is the sample table:
	   Column1,Column2,Column3
       1,      Tom,    200
       2,      Tim,    300
       2,      Bob,    600
       3,      Tim,    700
       4,      Bob,    900
       4,      Mike,   100
       4,      Kate,   200
       5,      Dup,    1000
       5,      Dup,    1000
	 * */
	public void testGetTableStatistic() {
		TableInstance t = TableInstanceReader.readTableFromFile("./dat/testtablestatistics/key_2columns");
		System.out.println(t);
		int count = t.getCountOfSameKey("Column2", "Column1", 1); //the row is 0-based
		assertEquals(2, count);
		count = t.getCountOfSameKey("Column2", "Column1", 0);
		assertEquals(1, count);
		count = t.getCountOfSameKey("Column2", "Column1", 2);
		assertEquals(2, count);
		count = t.getCountOfSameKey("Column2", "Column1", 4);
		assertEquals(3, count);
		//test using the same column as key column
		count = t.getCountOfSameKey("Column2", "Column2", 1); //the row is 0-based
		assertEquals(2, count);
		count = t.getCountOfSameKey("Column2", "Column2", 0);
		assertEquals(1, count);
		count = t.getCountOfSameKey("Column2", "Column2", 2);
		assertEquals(2, count);
		count = t.getCountOfSameKey("Column2", "Column2", 4);
		assertEquals(2, count);
		count = t.getCountOfSameKey("Column3", "Column3", 0);
		assertEquals(2, count);
		//test the unique count
		count = t.getUniqueCountOfSameKey("Column2", "Column1", 1);
		assertEquals(2, count);
		count = t.getUniqueCountOfSameKey("Column2", "Column1", 7);
		assertEquals(1, count);
		count = t.getUniqueCountOfSameKey("Column2", "Column2", 1); //the row is 0-based
		assertEquals(1, count);
		count = t.getUniqueCountOfSameKey("Column3", "Column2", 1); //the row is 0-based
		assertEquals(2, count);
		count = t.getUniqueCountOfSameKey("Column3", "Column2", 8); //the row is 0-based
		assertEquals(1, count);
		//test the sum
		int sum = t.getSumOfSameKey("Column3", "Column1", 0);
		assertEquals(200, sum);
		sum = t.getSumOfSameKey("Column3", "Column1", 1);
		assertEquals(900, sum);
		sum = t.getSumOfSameKey("Column3", "Column1", 4);
		assertEquals(1200, sum);
		sum = t.getSumOfSameKey("Column3", "Column3", 6);
		assertEquals(400, sum);
		//test the max
		int max = t.getMaxOfSameKey("Column3", "Column1", 1);
		assertEquals(600, max);
		max = t.getMaxOfSameKey("Column3", "Column1", 6);
		assertEquals(900, max);
		max = t.getMaxOfSameKey("Column3", "Column1", 3);
		assertEquals(700, max);
		//test the min
		int min = t.getMinOfSameKey("Column3", "Column1", 1);
		assertEquals(300, min);
		min = t.getMinOfSameKey("Column3", "Column1", 6);
		assertEquals(100, min);
		min = t.getMinOfSameKey("Column3", "Column1", 3);
		assertEquals(700, min);
		//test the avg
		int avg = t.getAvgOfSameKey("Column3", "Column1", 1);
		assertEquals(450, avg);
		avg = t.getAvgOfSameKey("Column3", "Column1", 3);
		assertEquals(700, avg);
	}
	
	public void testGetTableContent() {
		TableInstance t = TableInstanceReader.readTableFromFile("./dat/testtablestatistics/key_2columns");
		String content = t.getTableContent();
		assertEquals(9, content.split(Globals.lineSep).length);
	}
	
	public void testGroupByMultiColumns() {
		TableInstance t = TableInstanceReader.readTableFromFile("./dat/groupbymulticolumns/class_enroll.txt");
		System.out.println(t);
		int count = t.getCountOfSameKey("course_name", new String[]{"student_name", "room"}, 0);
		System.out.println(count);
		assertEquals(count, 2);
		
		count = t.getCountOfSameKey("course_name", new String[]{"student_name", "room"}, 3);
		System.out.println(count);
		assertEquals(count, 1);
		
		count = t.getCountOfSameKey("course_name", new String[]{"student_name", "room"}, 6);
		System.out.println(count);
		assertEquals(count, 2);
		count = t.getUniqueCountOfSameKey("course_name", new String[]{"student_name", "room"}, 6);
		System.out.println(count);
		assertEquals(count, 1);
	}
}
