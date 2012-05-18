package edu.washington.cs.sqlsynth.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import plume.Pair;

import edu.washington.cs.sqlsynth.entity.TableColumn;
import edu.washington.cs.sqlsynth.entity.TableInstance;
import edu.washington.cs.sqlsynth.util.TableInstanceReader;
import edu.washington.cs.sqlsynth.util.Utils;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestDatabaseConnection extends TestCase {

	public static Test suite() {
		return new TestSuite(TestDatabaseConnection.class);
	}
	
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
	
	public void testJoinTable() {
		TableInstance input1 = TableInstanceReader.readTableFromFile("./dat/id_name");
		TableInstance input2 = TableInstanceReader.readTableFromFile("./dat/id_salary");
		TableInstance output = TableInstanceReader.readTableFromFile("./dat/id_name_salary");
		
		Collection<TableInstance> tables = new LinkedList<TableInstance>();
		tables.add(input1);
		tables.add(input2);
		
		List<Pair<TableColumn, TableColumn>> joinColumns = new LinkedList<Pair<TableColumn, TableColumn>>();
		TableColumn c1 = input1.getColumnByName("ID_key");
		TableColumn c2 = input2.getColumnByName("ID_key");
		Utils.checkNotNull(c1);
		Utils.checkNotNull(c2);
		joinColumns.add(new Pair<TableColumn, TableColumn>(c1, c2));
		
		TableInstance t = DbConnector.instance().joinTable(tables, joinColumns);
		System.out.println(t);
	}
}
