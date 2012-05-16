package edu.washington.cs.sqlsynth.db;

import java.sql.Connection;
import java.sql.DriverManager;

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
	
}
