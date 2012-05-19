package edu.washington.cs.sqlsynth.util;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import plume.Pair;

import edu.washington.cs.sqlsynth.db.DbConnector;
import edu.washington.cs.sqlsynth.entity.TableColumn;
import edu.washington.cs.sqlsynth.entity.TableInstance;

public class TableUtils {
	public static TableColumn findFirstMatchedColumn(String columnName,
			Collection<TableInstance> inputTables) {
		for(TableInstance instance : inputTables) {
			TableColumn c = instance.getColumnByName(columnName);
			if(c != null) {
				return c;
			}
		}
		return null;
	}
	
	public static TableInstance findFirstTableWithMatchedColumn(String columnName,
			Collection<TableInstance> inputTables) {
		for(TableInstance instance : inputTables) {
			TableColumn c = instance.getColumnByName(columnName);
			if(c != null) {
				return  instance;
			}
		}
		return null;
	}
	
	public static boolean sameType(TableColumn c1, TableColumn c2) {
		return c1.getType().equals(c2.getType());
	}
	
	//enumerate all possible joining conditions
	public static List<TableInstance> joinTables(Collection<TableInstance> tables, Collection<Pair<TableColumn, TableColumn>> joinColumns) {
		List<TableInstance> list = new LinkedList<TableInstance>();
		if(joinColumns.isEmpty()) {
		    list.add(DbConnector.instance().joinTable(tables, joinColumns));
		} else {
			Collection<Collection<Pair<TableColumn, TableColumn>>> allSubs = Maths.allSubset(joinColumns);
			for(Collection<Pair<TableColumn, TableColumn>> js : allSubs) {
				list.add(DbConnector.instance().joinTable(tables, js));
			}
		}
		Log.logln("Number of tables after join: " + list.size());
		return list;
	}
}
