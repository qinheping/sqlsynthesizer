package edu.washington.cs.sqlsynth.util;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
				if(DbConnector.instance().areAllTablesUsedInJoining(tables, js)) {
				    list.add(DbConnector.instance().joinTable(tables, js));
				} else {
					Log.logln("Skip the case that of joining: " + joinColumns);
				}
			}
		}
		Log.logln("Number of tables after join: " + list.size());
		return list;
	}
	
	//select which column to use by alphametic ranking
	//in the joining conditions: a.id == b.id, both id should not
	//appear in the result at the same time, thus, we rank the their name
	//by the alphametic ranking, e.g.,  a.key ranks higher than b.key
	public TableColumn selectTableColumns(Collection<TableColumn> coll) {
		Utils.checkTrue(!coll.isEmpty());
		Map<String, TableColumn> map = new HashMap<String, TableColumn>();
		List<String> columnNames = new LinkedList<String>();
		for(TableColumn tc : coll) {
			String fullName = tc.getFullName();
			Utils.checkTrue(!map.containsKey(fullName));
			columnNames.add(fullName);
			map.put(fullName, tc);
		}
		String selectedName = this.selectTableNames(columnNames);
		return map.get(selectedName);
	}
	
	public String selectTableNames(Collection<String> names) {
		Utils.checkTrue(!names.isEmpty());
		List<String> list = new LinkedList<String>(names);
		Collections.sort(list);
		return list.get(0);
	}
	
	//is table t1 a part table t2?
	public static boolean subsume(TableInstance t1, TableInstance t2) {
		String t1str = t1.getTableContent();
		String t2str = t2.getTableContent() ;
		return subsume(t1str, t2str);
	}
	
	//t1str is the table dumpm is t1str a part t2str
	public static boolean subsume(String t1str, String t2str) {
		String[] t1strs = t1str.split(Globals.lineSep);
		String[] t2strs = t2str.split(Globals.lineSep);
		
		Set<String> set1 = new HashSet<String>();
		Set<String> set2 = new HashSet<String>();
		
		for(String s1 : t1strs) {
			set1.add(s1);
		}
		for(String s2 : t2strs) {
			set2.add(s2);
		}
		
		return set2.containsAll(set1);
	}
}
