package edu.washington.cs.sqlsynth.util;

import java.util.Collection;

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
}
