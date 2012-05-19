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
		Pair<TableColumn, TableColumn> p1 = this.getKeyPairs(tables);
		List<Pair<TableColumn, TableColumn>> p2list = this.getNameMatchedPairs(tables);
		if(p1 != null) {
			skeleton.addJoinColumns(p1);
		}
		if(p2list != null && !p2list.isEmpty()) {
			for(Pair<TableColumn, TableColumn> p2 : p2list) {
			    skeleton.addJoinColumns(p2);
			}
		}
		
		//all projection columns
		int index = 0;
		for(TableColumn column : outputColumns) {
			TableColumn c = TableUtils.findFirstMatchedColumn(column.getColumnName(), this.inputTables);
			if(c != null) {
				skeleton.setProjectColumn(index, c);
			}
			index++;
		}
		
		return skeleton;
	}
	
	//FIXME
	private Pair<TableColumn, TableColumn> getKeyPairs(List<TableInstance> tables) {
		if(tables.size() == 1) {
			return null;
		}
		Utils.checkTrue(tables.size() == 2);
		TableColumn key1 = tables.get(0).getKeyColumns().get(0);
		TableColumn key2 = tables.get(1).getKeyColumns().get(0);
		return new Pair<TableColumn, TableColumn>(key1, key2);
	}
	
	//FIXME
	private List<Pair<TableColumn, TableColumn>> getNameMatchedPairs(List<TableInstance> tables) {
		if(tables.size() == 1) {
			return null;
		}
		Utils.checkTrue(tables.size() == 2);
		
		List<Pair<TableColumn, TableColumn>> pairs = new LinkedList<Pair<TableColumn, TableColumn>>();
		
		return null;
	}
}