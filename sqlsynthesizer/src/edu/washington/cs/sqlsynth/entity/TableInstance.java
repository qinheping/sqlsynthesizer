package edu.washington.cs.sqlsynth.entity;

import java.util.LinkedList;
import java.util.List;

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
		this.columns.add(column);
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
				contents[i] = (String) (contents[i] == null
				    ? column.getValues().get(i)
				    :  contents[i] + TableInstanceReader.SEP + column.getValues().get(i));
			}
		}
		for(String content : contents) {
			sb.append(content);
			sb.append(Globals.lineSep);
		}
		
		return sb.toString();
	}
}