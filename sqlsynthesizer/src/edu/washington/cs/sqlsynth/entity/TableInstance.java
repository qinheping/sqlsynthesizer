package edu.washington.cs.sqlsynth.entity;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import edu.washington.cs.sqlsynth.util.Globals;
import edu.washington.cs.sqlsynth.util.TableInstanceReader;
import edu.washington.cs.sqlsynth.util.Utils;

public class TableInstance {

	private final String tableName;
	
	private List<TableColumn> columns = new LinkedList<TableColumn>();
	
	private int rowNum = -1;
	
	public TableInstance(String tablename) {
		this.tableName = tablename;
	}
	
	public String getTableName() {
		return this.tableName;
	}
	
	public List<TableColumn> getColumns() {
		return this.columns;
	}
	
	public TableColumn getColumn(int i) {
		Utils.checkTrue(i >= 0 && i < this.getColumnNum());
		return this.columns.get(i);
	}
	
	public int getColumnNum() {
		return this.getColumns().size();
	}
	
	//it is a 1-based
	public int getRowNum() {
		Utils.checkTrue(rowNum > -1);
		return this.rowNum;
	}
	
	public List<Object> getRowValuesWithQuoate(int i) {
		Utils.checkTrue(i >= 0 && i < this.getRowNum());
		List<Object> values = new LinkedList<Object>();
		for(TableColumn c : this.columns) {
			values.add(c.getValueWithQuoate(i));
		}
		return values;
	}
	
	public List<Object> getRowValues(int i) {
		Utils.checkTrue(i >= 0 && i < this.getRowNum(), "The table only has: "
				+ this.getRowNum() + " rows, but you want to fetch row: " + i);
		List<Object> values = new LinkedList<Object>();
		for(TableColumn c : this.columns) {
			values.add(c.getValue(i));
		}
		return values;
	}
	
	public void addColumn(TableColumn column) {
		Utils.checkNotNull(column);
		Utils.checkTrue(column.getTableName().equals(tableName));
		if(rowNum == -1) {
			rowNum = column.getRowNumber();
		} else {
			Utils.checkTrue(column.getRowNumber() == rowNum,
					"The given column's row num: " + column.getRowNumber()
					+ " != rowNum: " + rowNum);
		}
		//check no columns with the same name has been added
		Set<String> existingColumns = new HashSet<String>();
		for(TableColumn c : this.columns) {
			existingColumns.add(c.getColumnName());
		}
//		Utils.checkTrue(!existingColumns.contains(column.getColumnName()),
//				"You can not have two columns with the same name: " + column.getColumnName());
		this.columns.add(column);
	}
	
	public boolean hasColumn(String columnName) {
		return this.getColumnByName(columnName) != null;
	}
	
	public TableColumn getColumnByName(String columnName) {
		for(TableColumn c : this.columns) {
			if(c.getColumnName().equals(columnName)) {
				return c;
			}
		}
		return null;
	}
	
	public List<TableColumn> getKeyColumns() {
		List<TableColumn> keys = new LinkedList<TableColumn>();
		for(TableColumn c : this.columns) {
			if(c.isKey()) {
				keys.add(c);
			}
		}
		//FIXME
		Utils.checkTrue(keys.size() < 2, "At most 1 key is allowed, but table: " + this.tableName
				+ " has: " + keys.size() + " keys");
		return keys;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.tableName);
		sb.append(Globals.lineSep);
		//dump the table content
		int rowNum = this.rowNum;
		for(TableColumn column : this.columns) {
			sb.append(column.getColumnName());
			sb.append(" (isKey? ");
			sb.append(column.isKey());
			sb.append(", type: ");
			sb.append(column.getType());
			sb.append(" )");
			sb.append(TableInstanceReader.SEP);
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append(Globals.lineSep);
		
		//dump the content
		String[] contents = new String[rowNum];
		for(TableColumn column : this.columns) {
			for(int i = 0; i < rowNum; i++) {
				contents[i] = "" + (contents[i] == null
				    ? column.getValues().get(i)
				    :  (contents[i] + TableInstanceReader.SEP
				        + column.getValues().get(i)));
			}
		}
		for(String content : contents) {
			sb.append(content);
			sb.append(Globals.lineSep);
		}
		
		return sb.toString();
	}
}