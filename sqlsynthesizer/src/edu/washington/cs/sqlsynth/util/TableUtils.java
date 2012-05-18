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
	
	//XXFIXME only consider one possibility
	public static List<TableInstance> joinTables(Collection<TableInstance> tables, Collection<Pair<TableColumn, TableColumn>> joinColumns) {
		//XXX FIXME need to enumerate all possible joining conditions
		List<TableInstance> list = new LinkedList<TableInstance>();
		list.add(DbConnector.instance().joinTable(tables, joinColumns));
		return list;
	}
}
