package edu.washington.cs.sqlsynth.entity;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.washington.cs.sqlsynth.util.Utils;

public class SQLSkeleton {
	
	//can have repetition
	private List<TableInstance> tables = new LinkedList<TableInstance>();
	private List<TableColumn> joinColumns = new LinkedList<TableColumn>();
	
	//the projections
	private Map<Integer, TableColumn> projectColumns = new LinkedHashMap<Integer, TableColumn>();
	private final int numberOfProjectColumns;
	
	public SQLSkeleton(int numOfProjectColumns) {
		Utils.checkTrue(numOfProjectColumns > 0);
		this.numberOfProjectColumns = numOfProjectColumns;
	}
	
	public List<TableInstance> getTables() {
		return tables;
	}
	public List<TableColumn> getJoinColumns() {
		return joinColumns;
	}
	public Map<Integer, TableColumn> getProjectColumns() {
		return projectColumns;
	}
	
	public int getNumOfProjectColumns() {
		return this.numberOfProjectColumns;
	}
}