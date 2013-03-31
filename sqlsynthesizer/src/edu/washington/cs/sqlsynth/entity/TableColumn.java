package edu.washington.cs.sqlsynth.entity;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import edu.washington.cs.sqlsynth.util.Globals;
import edu.washington.cs.sqlsynth.util.Utils;

public class TableColumn {

	//only supports two types: integer or string
	public enum ColumnType{Integer, String};
	
	private final String tableName;
	private final String columnName;
	private final ColumnType type;
	private final boolean isKey;
	private final List<Object> values = new LinkedList<Object>();
	
	public TableColumn(String tableName, String columnName, ColumnType type, boolean isKey) {
		this.tableName = tableName;
		this.columnName = columnName;
		this.type = type;
		this.isKey = isKey;
	}
	
	public void addValue(Object o) {
		Utils.checkNotNull(o);
		if(this.isIntegerType()) {
			Utils.checkTrue(Utils.isInteger(o.toString()));
		}
		this.values.add(o);
	}
	
	public void addValues(Collection<Object> os) {
		for(Object o : os) {
			this.addValue(o);
		}
	}
	
	public boolean isStringType() {
		return this.type.equals(ColumnType.String);
	}
	
	public boolean isIntegerType() {
		return this.type.equals(ColumnType.Integer);
	}

	public String getTableName() {
		return tableName;
	}

	public String getColumnName() {
		return columnName;
	}
	
	public String getFullName() {
		return tableName + "." + columnName;
	}
	
	/**
	 * This is MySQL specific, since the backend database
	 * of the current implementation is MySQL.
	 * */
	public String getMySQLColumnType() {
		if(this.isIntegerType()) {
			return "Integer";
		} else if (this.isStringType()) {
			return "Varchar(10)";
		} else {
			throw new Error();
		}
	}

	public ColumnType getType() {
		return type;
	}

	public boolean isKey() {
		return isKey;
	}
	
	public int getRowNumber() {
		return this.values.size();
	}
	
	/**
	 * Must use quoate to generate legal sql queries. For example,
	 * select * from table where name = 'jack' (the quoate must be here)
	 * */
	public Object getValueWithQuoate(int index) {
		String v = this.values.get(index) + "";
		if(this.isStringType()) {
			return "\'" + v + "\'";
		}
		return v;
	}
	
	public Object getValue(int index) {
		return this.values.get(index);
	}
	
	public List<Object> getValues() {
		return this.values;
	}
	
	public List<String> getStringValues() {
		Utils.checkTrue(this.isStringType(), "The current column is not string type");
		List<String> retValues = new LinkedList<String>();
		for(Object o : this.values) {
			retValues.add(o.toString());
		}
		return retValues;
	}
	
	public List<Integer> getIntegerValues() {
		Utils.checkTrue(this.isIntegerType(), "The current column is not integer type");
		List<Integer> retValues = new LinkedList<Integer>();
		for(Object o : this.values) {
			retValues.add(Integer.parseInt(o.toString().trim()));
		}
		return retValues;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.tableName);
		sb.append(".");
		sb.append(this.columnName);
		sb.append(" (isKey? ");
		sb.append(isKey);
		sb.append(", type: ");
		sb.append(this.type);
		sb.append(")");
		sb.append(Globals.lineSep);
		
		for(Object v : this.values) {
			sb.append(v);
			sb.append(Globals.lineSep);
		}
		
		return sb.toString();
	}
}
