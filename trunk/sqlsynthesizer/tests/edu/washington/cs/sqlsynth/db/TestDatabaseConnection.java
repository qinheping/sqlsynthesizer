package edu.washington.cs.sqlsynth.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collection;
import java.util.LinkedList;

import edu.washington.cs.sqlsynth.entity.TableInstance;
import edu.washington.cs.sqlsynth.util.TableInstanceReader;

import junit.framework.TestCase;

public class TestDatabaseConnection extends TestCase {

	public void testPassConfigFile() {
		System.out.println(MySQLConfigReader.getURL());
		System.out.println(MySQLConfigReader.getDriver());
		System.out.println(MySQLConfigReader.getDbname());
		System.out.println(MySQLConfigReader.getUsername());
		System.out.println(MySQLConfigReader.getPassword());
	}
	
	public void testConnectToDb() {
		System.out.println("Connecting to my sql... ");
	    Connection conn = null;
	    String url = MySQLConfigReader.getURL();
	    String dbName = MySQLConfigReader.getDbname();
	    String driver = MySQLConfigReader.getDriver();
	    String userName = MySQLConfigReader.getUsername(); 
	    String password = MySQLConfigReader.getPassword();
	    System.out.println("URL: " + url);
		System.out.println("dbname: " + dbName);
		System.out.println("driver:  " + driver);
		System.out.println("username: " + userName);
		System.out.println("password: " + password);
	    try {
	      Class.forName(driver).newInstance();
	      conn = DriverManager.getConnection(url+dbName,userName,password);
	      System.out.println("Connected to the database");
	      conn.close();
	      System.out.println("Disconnected from database");
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
	}
	
	public void testCreateSampleTables() {
		TableInstance input1 = TableInstanceReader.readTableFromFile("./dat/id_name");
		TableInstance input2 = TableInstanceReader.readTableFromFile("./dat/id_salary");
		TableInstance output = TableInstanceReader.readTableFromFile("./dat/id_name_salary");
		
		Collection<TableInstance> tables = new LinkedList<TableInstance>();
		tables.add(input1);
		tables.add(input2);
		tables.add(output);
		
		DbConnector connector = DbConnector.instance();
		connector.initializeInputTables(tables);
	}
	
	public void testCheckSampleQuery() {
		testCreateSampleTables();
		TableInstance output = TableInstanceReader.readTableFromFile("./dat/id_name_salary");
		DbConnector connector = DbConnector.instance();
		String query = "select id_name.ID_key, id_name.Name, id_salary.Salary " +
				"from id_name, id_salary where id_name.ID_key=id_salary.ID_key " +
				"and id_salary.Salary=200";
		boolean same = connector.checkSQLQueryWithOutput(output, query);
		assertTrue(same);
		TableInstance input2 = TableInstanceReader.readTableFromFile("./dat/id_salary");
		boolean diff = connector.checkSQLQueryWithOutput(input2, query);
		assertTrue(!diff);
	}
}
