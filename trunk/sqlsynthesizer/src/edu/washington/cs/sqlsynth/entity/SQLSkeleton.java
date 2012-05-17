package edu.washington.cs.sqlsynth.entity;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import plume.Pair;

import edu.washington.cs.sqlsynth.util.Globals;
import edu.washington.cs.sqlsynth.util.Utils;

public class SQLSkeleton {
	
	//can have repetition
	private List<TableInstance> tables = new LinkedList<TableInstance>();
	//all equal join columns
	private List<Pair<TableColumn, TableColumn>> joinColumns = new LinkedList<Pair<TableColumn, TableColumn>>();
	
	//the projections, also zero based
	private Map<Integer, TableColumn> projectColumns = new LinkedHashMap<Integer, TableColumn>();
	private final int numberOfProjectColumns;
	
	public SQLSkeleton(int numOfProjectColumns) {
		Utils.checkTrue(numOfProjectColumns > 0);
		this.numberOfProjectColumns = numOfProjectColumns;
	}
	
	public List<TableInstance> getTables() {
		return tables;
	}
	
	public void addTable(TableInstance table) {
		this.tables.add(table);
	}
	
	public void addTables(Collection<TableInstance> tables) {
		this.tables.addAll(tables);
	}
	
	public int getJoinPairNum() {
		return this.joinColumns.size();
	}
	
	public List<Pair<TableColumn, TableColumn>> getJoinColumns() {
		return joinColumns;
	}
	
	public void addJoinColumns(TableColumn t1, TableColumn t2) {
		Pair<TableColumn, TableColumn> p = new Pair<TableColumn, TableColumn>(t1, t2);
		addJoinColumns(p);
	}
	
	public void addJoinColumns(Pair<TableColumn, TableColumn> p) {
		Utils.checkTrue(!this.joinColumns.contains(p));
		this.joinColumns.add(p);
	}
	
	public Map<Integer, TableColumn> getProjectColumns() {
		return projectColumns;
	}
	
	public void setProjectColumn(int index, TableColumn column) {
		Utils.checkTrue(!this.projectColumns.containsKey(index));
		this.projectColumns.put(index, column);
	}
	
	public int getNumOfProjectColumns() {
		return this.numberOfProjectColumns;
	}
	
	public String getAllJoinConditions() {
		if(this.joinColumns.isEmpty()) {
			return "";
		}
		StringBuilder sb = new StringBuilder();
		sb.append("(");
		int count = 0;
		for(Pair<TableColumn, TableColumn> p : this.joinColumns) {
			if(count != 0) {
				sb.append(" and ");
			}
			sb.append(p.a.getFullName());
			sb.append("=");
			sb.append(p.b.getFullName());
			count++;
		}
		sb.append(")");
		return sb.toString();
	}
	
	public String getJoinCondition(int i) {
		Utils.checkTrue(this.getJoinPairNum() > 0);
		Utils.checkTrue(i >= 0 && i < this.getJoinPairNum());
		int count = 0;
		for(Pair<TableColumn, TableColumn> p : this.joinColumns) {
			if(count == i) {
				return "(" + p.a.getFullName() + " = " + p.b.getFullName() + ")";
			}
			count++;
		}
		throw new Error("unreachable!");
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("tables: ");
		for(TableInstance t : this.getTables()) {
			sb.append(t.getTableName());
			sb.append("\t");
		}
		sb.append(Globals.lineSep);
		sb.append("Join column pairs: ");
		for(Pair<TableColumn, TableColumn> p : this.joinColumns) {
			sb.append(p.a.getFullName() + " join " + p.b.getFullName());
			sb.append("\t");
		}
		sb.append(Globals.lineSep);
		sb.append("Output columns: ");
		for(int index : this.getProjectColumns().keySet()) {
			sb.append(index);
			sb.append(":");
			sb.append(this.getProjectColumns().get(index).getFullName());
			sb.append("\t");
		}
		sb.append(Globals.lineSep);
		return sb.toString();
	}
}