package edu.washington.cs.sqlsynth.algorithms;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import plume.Pair;

import edu.washington.cs.sqlsynth.entity.SQLSkeleton;
import edu.washington.cs.sqlsynth.entity.TableColumn;
import edu.washington.cs.sqlsynth.entity.TableInstance;
import edu.washington.cs.sqlsynth.util.TableUtils;
import edu.washington.cs.sqlsynth.util.Utils;

/**
 * Infer possible columns by inspecting:
 * (1) key info
 * (2) column names
 * (3) types
 * (4) content
 * */
public class SQLSkeletonCreator {
	
	public static boolean matchingName = true;

	public final List<TableInstance> inputTables;
	public final TableInstance outputTable;
	
	public SQLSkeletonCreator(Collection<TableInstance> inputTables,
			TableInstance outputTable) {
		this.inputTables = new LinkedList<TableInstance>();
		this.inputTables.addAll(inputTables);
		this.outputTable = outputTable;
	}
	
	public SQLSkeleton inferSQLSkeleton() {
		
		SQLSkeleton skeleton = new SQLSkeleton(this.inputTables, this.outputTable);
		
		//first get all tables
		List<TableInstance> tables = new LinkedList<TableInstance>();
		List<TableColumn> outputColumns = outputTable.getColumns();
		//FIXME no consider repetitive columns
//		for(TableColumn column : outputColumns) {
//			TableInstance t = TableUtils.findFirstTableWithMatchedColumn(column.getColumnName(), this.inputTables);
//			if(t != null && !tables.contains(t)) {
//				tables.add(t);
//			}
//		}
		tables.addAll(this.inputTables);
		skeleton.addTables(tables);
		Utils.checkTrue(skeleton.getTables().size() >= this.inputTables.size());
		
		//all possible joining conditions
		//join with keys
		List<Pair<TableColumn, TableColumn>> p1list = this.getKeyPairs(tables);
		//join by columns, with name
		List<Pair<TableColumn, TableColumn>> p2list = this.getNameMatchedPairs(tables);
		
		//FIXME whether we should add columns with different names
		//but the same type? controlled by the flag of matchingNames.
		
		if(p1list != null && !p1list.isEmpty()) {
			for(Pair<TableColumn, TableColumn> p1 : p1list) {
			    skeleton.addJoinColumns(p1);
			}
		}
		if(p2list != null && !p2list.isEmpty()) {
			for(Pair<TableColumn, TableColumn> p2 : p2list) {
			    skeleton.addJoinColumns(p2);
			}
		}
		
		//all projection columns
		int index = 0;
		for(TableColumn column : outputColumns) {
			//the first column with the same name
			TableColumn c = TableUtils.findFirstMatchedColumn(column.getColumnName(), this.inputTables);
			if(c != null) {
				skeleton.setProjectColumn(index, c);
			}
			index++;
		}
		
		return skeleton;
	}
	
	/**
	 * Return all pairs of columns with the same type, and both are keys.
	 * */
	private List<Pair<TableColumn, TableColumn>> getKeyPairs(List<TableInstance> tables) {
		List<Pair<TableColumn, TableColumn>> pairList = new LinkedList<Pair<TableColumn, TableColumn>>();
		if(tables.size() == 1) {
			return pairList;
		}
		
		List<TableColumn> allKeys = new LinkedList<TableColumn>();
		for(TableInstance t : tables) {
			allKeys.addAll(t.getKeyColumns());
		}
		
		for(int i = 0; i < allKeys.size(); i++) {
			for(int j = i + 1; j < allKeys.size(); j++) {
				TableColumn key1 = allKeys.get(i);
				TableColumn key2 = allKeys.get(j);
				if(TableUtils.sameType(key1, key2)) {
					Pair<TableColumn, TableColumn> p = new Pair<TableColumn, TableColumn>(key1, key2);
					pairList.add(p);
				}
			}
		}
		
		return pairList;
	}
	
	/**
	 * Return all pairs that is not in the same table but with the same type
	 * and the same column name
	 * */
	private List<Pair<TableColumn, TableColumn>> getNameMatchedPairs(List<TableInstance> tables) {
		if(tables.size() == 1) {
			return null;
		}
		
		List<Pair<TableColumn, TableColumn>> pairs = new LinkedList<Pair<TableColumn, TableColumn>>();
		List<TableColumn> allColumns = new LinkedList<TableColumn>();
		for(TableInstance t : tables) {
			allColumns.addAll(t.getColumns());
		}
		for(int i = 0; i < allColumns.size(); i++) {
			for(int j = i; j < allColumns.size(); j++) {
				TableColumn c1 = allColumns.get(i);
				TableColumn c2 = allColumns.get(j);
				if(c1.isKey() && c2.isKey()) {
					continue;
				}
				if(TableUtils.sameType(c1, c2)
						&& !c1.getTableName().equals(c2.getTableName())
						&& this.columnMatched(c1, c2)) {
					Pair<TableColumn, TableColumn> p = new Pair<TableColumn, TableColumn>(c1, c2);
					pairs.add(p);
				}
			}
		}
		
		return pairs;
	}
	
	private boolean columnMatched(TableColumn c1, TableColumn c2) {
		Utils.checkTrue(!c1.getFullName().equals(c2.getFullName()));
		if(matchingName) {
		    return c1.getColumnName().equals(c2.getColumnName());
		}
		return true;
	}
}
