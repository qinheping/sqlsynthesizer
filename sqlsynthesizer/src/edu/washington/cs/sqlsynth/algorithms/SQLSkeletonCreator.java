package edu.washington.cs.sqlsynth.algorithms;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import plume.Pair;

import edu.washington.cs.sqlsynth.entity.SQLSkeleton;
import edu.washington.cs.sqlsynth.entity.TableColumn;
import edu.washington.cs.sqlsynth.entity.TableInstance;
import edu.washington.cs.sqlsynth.util.Utils;

public class SQLSkeletonCreator {

	public final List<TableInstance> inputTables;
	public final TableInstance outputTable;
	
	public SQLSkeletonCreator(Collection<TableInstance> inputTables,
			TableInstance outputTable) {
		this.inputTables = new LinkedList<TableInstance>();
		this.inputTables.addAll(inputTables);
		this.outputTable = outputTable;
	}
	
	public SQLSkeleton inferSQLSkeleton() {
		int outputColumnNum = this.outputTable.getColumnNum();
		SQLSkeleton skeleton = new SQLSkeleton(outputColumnNum);
		
		//first get all tables
		List<TableInstance> tables = new LinkedList<TableInstance>();
		List<TableColumn> outputColumns = outputTable.getColumns();
		//FIXME no consider repetitive columns
		for(TableColumn column : outputColumns) {
			TableInstance t = this.findFirstTableWithMatchedColumn(column.getColumnName());
			if(t != null && !tables.contains(t)) {
				tables.add(t);
			}
		}
		skeleton.addTables(tables);
		
		//all possible joining conditions
		Pair<TableColumn, TableColumn> p1 = this.getKeyPairs(tables);
		Pair<TableColumn, TableColumn> p2 = this.getNameMatchedPairs(tables);
		if(p1 != null) {
			skeleton.addJoinColumns(p1);
		}
		if(p2 != null) {
			skeleton.addJoinColumns(p2);
		}
		
		//all projection columns
		int index = 0;
		for(TableColumn column : outputColumns) {
			TableColumn c = this.findFirstMatchedColumn(column.getColumnName());
			if(c != null) {
				skeleton.setProjectColumn(index, c);
			}
			index++;
		}
		
		return skeleton;
	}
	
	private TableInstance findFirstTableWithMatchedColumn(String columnName) {
		for(TableInstance instance : this.inputTables) {
			TableColumn c = instance.getColumnByName(columnName);
			if(c != null) {
				return  instance;
			}
		}
		return null;
	}
	
	private TableColumn findFirstMatchedColumn(String columnName) {
		for(TableInstance instance : this.inputTables) {
			TableColumn c = instance.getColumnByName(columnName);
			if(c != null) {
				return c;
			}
		}
		return null;
	}
	
	//FIXME
	private Pair<TableColumn, TableColumn> getKeyPairs(List<TableInstance> tables) {
		Utils.checkTrue(tables.size() == 2);
		TableColumn key1 = tables.get(0).getKeyColumns().get(0);
		TableColumn key2 = tables.get(1).getKeyColumns().get(0);
		return new Pair<TableColumn, TableColumn>(key1, key2);
	}
	
	//FIXME
	private Pair<TableColumn, TableColumn> getNameMatchedPairs(List<TableInstance> tables) {
		return null;
	}
}
