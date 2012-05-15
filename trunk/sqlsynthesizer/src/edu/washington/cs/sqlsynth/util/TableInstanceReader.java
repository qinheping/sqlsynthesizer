package edu.washington.cs.sqlsynth.util;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import edu.washington.cs.sqlsynth.entity.TableColumn;
import edu.washington.cs.sqlsynth.entity.TableColumn.ColumnType;
import edu.washington.cs.sqlsynth.entity.TableInstance;

public class TableInstanceReader {
	
	public static String SEP = ",";
	public static String KEY = "_key";
	
	public final String fileName;
	private TableInstance instance = null;
	
	public TableInstanceReader(String fileName) {
		this.fileName = fileName;
	}
	
	public TableInstance getTableInstance() {
		if(instance == null) {
			instance = parseFile();
		}
		return instance;
	}
	
	private TableInstance parseFile() {
		List<String> contents = Files.readWholeNoExp(this.fileName);
		String tableName = new File(this.fileName).getName();
		String[] columnNames = null;
		List<String[]> tuples = new LinkedList<String[]>();
		for(String content : contents) {
			if(content.trim().isEmpty()) {
				continue;
			}
			if(columnNames == null) {
				 columnNames = content.trim().split(SEP);
				 continue;
			}
			String[] tupleValues = content.trim().split(SEP);
			tuples.add(tupleValues);
		}
		//create the table instance
		TableInstance instance = new TableInstance(tableName);
		int columnNum = columnNames.length;
		for(int i = 0; i < columnNum; i++) {
			boolean isKey = columnNames[i].endsWith(KEY);
			ColumnType type = ColumnType.Integer;
			List<Object> values = new LinkedList<Object>();
			//decide type and collect values
			for(String[] vs : tuples) {
				String v = vs[i];
				values.add(v);
				if(!Utils.isInteger(v)) {
					type = ColumnType.String;
				}
			}
			TableColumn column = new TableColumn(tableName, columnNames[i],
					type, isKey);
			column.addValues(values);
			//add the column to the table
			instance.addColumn(column);
		}
		
		return instance;
	}
	
	public List<TableColumn> getTableColumns() {
		TableInstance instance = this.getTableInstance();
		return instance.getColumns();
	}
	
}