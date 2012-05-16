package edu.washington.cs.sqlsynth.db;

import java.io.FileInputStream;
import java.util.Properties;

import edu.washington.cs.sqlsynth.util.Utils;

public class MySQLConfigReader {

	private static String configFile = "./dat/mysql.config";
	
	private static String url = null;
	private static String dbname = null;
	private static String driver = null;
	private static String username = null;
	private static String password = null;
	
	public static void setConfigFile(String fileName) {
		configFile = fileName;
	}
	
	public static String getURL() {
		if(url == null) {
			readConfigFileAndInitFields();
		}
		return url;
	}
	
	public static String getDbname() {
		if(dbname == null) {
			readConfigFileAndInitFields();
		}
		return dbname;
	}
	
	public static String getDriver() {
		if(driver == null) {
			readConfigFileAndInitFields();
		}
		return driver;
	}
	
	public static String getUsername() {
		if(username == null) {
			readConfigFileAndInitFields();
		}
		return username;
	}
	
	public static String getPassword() {
		if(password == null) {
			readConfigFileAndInitFields();
		}
		return password;
	}
	
	private static void readConfigFileAndInitFields() {
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream(configFile));
		} catch (Exception e) {
			throw new Error(e);
		}
		url = prop.getProperty("url");
		dbname = prop.getProperty("dbname");
		driver = prop.getProperty("driver");
		username = prop.getProperty("username");
		password = prop.getProperty("password");
		
		Utils.checkNotNull(url);
		Utils.checkNotNull(dbname);
		Utils.checkNotNull(driver);
		Utils.checkNotNull(username);
		Utils.checkNotNull(password);
	}
	
}
