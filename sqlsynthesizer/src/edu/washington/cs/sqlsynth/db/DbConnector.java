package edu.washington.cs.sqlsynth.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collection;
import java.util.List;

import edu.washington.cs.sqlsynth.entity.SQLQuery;
import edu.washington.cs.sqlsynth.entity.TableColumn;
import edu.washington.cs.sqlsynth.entity.TableInstance;
import edu.washington.cs.sqlsynth.util.Globals;
import edu.washington.cs.sqlsynth.util.Utils;

public class DbConnector {

	private Connection con = null;
	
	private static DbConnector c = null;
	
	private DbConnector() {}
	
	public static DbConnector instance() {
		if(c == null) {
			c = new DbConnector();
		}
		return c;
	}

	public void initializeInputTables(Collection<TableInstance> inputTables) {
		if(con == null) {
		    connect();
		}
		for (TableInstance t : inputTables) {
			this.initializeTable(t);
		}
	}

	public boolean checkSQLQueryWithOutput(TableInstance output, SQLQuery sql) {
		return checkSQLQueryWithOutput(output, sql.toSQLString());
	}
	private static String COMMA = ",";
	boolean checkSQLQueryWithOutput(TableInstance output, String sql) {
		//convert all to string for comparison
		//FIXME may have co-incident matching
		StringBuilder outputSb = new StringBuilder();
		for(int i = 0; i < output.getRowNum(); i++) {
			List<Object> objs = output.getRowValues(i);
			if(i != 0) {
				outputSb.append(Globals.lineSep);
			}
			for(int index = 0; index < objs.size(); index++) {
				if(index != 0) {
					outputSb.append(COMMA);
				}
				outputSb.append(objs.get(index) + "");
			}
		}
		//query the database to check if the results are the same!
		ResultSet rs = this.executeQuery(con, sql);
		String queryResultStr = tablizeResultSet(rs);
		System.out.println("output sb: ");
		System.out.println(outputSb.toString());
		System.out.println("query result: ");
		System.out.println(queryResultStr.toString());
		return outputSb.toString().equals(queryResultStr);
	}

	public void connect() {
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
			con = DriverManager.getConnection(url + dbName, userName, password);
			System.out.println("Connected to the database");
		} catch (Exception e) {
			throw new Error(e);
		}
	}

	public void disconnect() {
		if (con != null) {
			try {
				con.close();
			} catch (SQLException e) {
				throw new Error();
			}
		}
	}
	
	void initializeTable(TableInstance table) {
		String tableName = table.getTableName();
		if(this.isTableExist(tableName)) {
			this.deleteAllTableContent(tableName);
		} else {
			//then create the table
			StringBuilder sql = new StringBuilder();
			sql.append("create table ");
			sql.append(tableName);
			sql.append(" (");
			for(int i = 0; i < table.getColumnNum(); i++) {
				if(i != 0) {
					sql.append(" , ");
				}
				TableColumn c = table.getColumns().get(i);
				sql.append(c.getColumnName());
				sql.append(" ");
				sql.append(c.getMySQLColumnType());
				
			}
			sql.append(" )");
			this.executeSQL(con, sql.toString());
		}
		
		//then insert the data
		for(int i = 0; i < table.getRowNum(); i++) {
			List<Object> values = table.getRowValuesWithQuoate(i);
			StringBuilder insert = new StringBuilder();
			insert.append("insert into ");
			insert.append(tableName);
			insert.append(" values(");
			for(int index = 0; index < values.size(); index++) {
				if(index != 0) {
					insert.append(", ");
				}
				insert.append(values.get(index));
			}
			insert.append(" )");
			this.executeSQL(con, insert.toString());
		}
	}
	
	boolean isTableExist(String tableName) {
		String sql = "select * from " + tableName;
		Statement s = null;
		try {
			s = con.createStatement ();
		} catch (SQLException e) {
			throw new Error(e);
		}
		try {
			s.executeQuery(sql);
		} catch (SQLException e) {
			return false;
		}
		try {
			s.close();
		} catch (SQLException e) {
			throw new Error(e);
		}
		return true;
	}
	
	void deleteAllTableContent(String tableName) {
		String delete = "delete from " + tableName;
		try {
			System.out.println("executing: " + delete);
			Statement s = con.createStatement ();
			s.executeUpdate(delete);
			s.close();
		} catch (SQLException e) {
			throw new Error(e);
		}
	}
	
	void dropTable(String tableName) {
		String drop = "drop table " + tableName;
		try {
			System.out.println("executing: " + drop);
			Statement s = con.createStatement ();
			s.executeUpdate(drop);
			s.close();
		} catch (SQLException e) {
			throw new Error(e);
		}
	}
	
	private void executeSQL(Connection con, String sql) {
		try {
			System.out.println("executing: " + sql.toString());
			Statement s = con.createStatement ();
			s.execute(sql);
			s.close();
		} catch (SQLException e) {
			throw new Error(e);
		}
	}
	
	private ResultSet executeQuery(Connection con, String sql) {
		try {
			System.out.println("executing: " + sql.toString());
			Statement s = con.createStatement ();
			ResultSet rs = s.executeQuery(sql);
//			s.close();
			return rs;
		} catch (SQLException e) {
			throw new Error(e);
		}
	}
	
	private String tablizeResultSet(ResultSet rs) {
		try {
			Utils.checkTrue(!rs.isClosed());
			ResultSetMetaData meta = rs.getMetaData();
			int columnCount = meta.getColumnCount();
			int count = 0;
			StringBuilder sb = new StringBuilder();
			while(rs.next()) {
				if(count != 0) {
					sb.append(Globals.lineSep);
				}
				for(int i = 0; i < columnCount; i++) {
					if(i != 0) {
						sb.append(COMMA);
					}
//					System.out.println(i);
					int t = meta.getColumnType(i + 1); //note it is 1-based
					String v = null;
					if(t == Types.INTEGER) {
						v = rs.getInt(i + 1) + "";
					} else if (t == Types.VARCHAR) {
						v = rs.getString(i + 1);
					} else {
						Utils.checkTrue(false);
					}
					Utils.checkNotNull(v);
					sb.append(v);
				}
				count ++;
			}
			return sb.toString();
		} catch (SQLException e) {
			throw new Error(e);
		}
		
	}
}
