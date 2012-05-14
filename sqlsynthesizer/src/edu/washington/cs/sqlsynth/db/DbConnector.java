package edu.washington.cs.sqlsynth.db;

import java.util.List;

import edu.washington.cs.sqlsynth.entity.SQLQuery;
import edu.washington.cs.sqlsynth.entity.TableInstance;

public class DbConnector {

	public void initializeInputTables(List<TableInstance> inputTables) {
		//call mysql to create tables in mysql
	}
	
	public boolean checkSQLQueryWithOutput(TableInstance output, SQLQuery sql) {
		throw new RuntimeException();
	}
}
