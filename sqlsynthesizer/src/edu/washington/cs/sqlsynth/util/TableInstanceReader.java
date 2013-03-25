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
	
	public static String COLUMN_NAME_TBD = "COLUMN_NAME_TBD";
	
	public final String fileName;
	private boolean withSchema = true;
	private TableInstance instance = null;
	
	public static TableInstance readTableFromFile(String fileName) {
		TableInstanceReader reader = new TableInstanceReader(fileName);
		TableInstance table = reader.getTableInstance();
		return table;
	}
	
	public static TableInstance readTableFromFileWithoutSchema(String fileName) {
		TableInstanceReader reader = new TableInstanceReader(fileName);
		reader.setSchema(true);
		TableInstance table = reader.getTableInstance();
		return table;
	}
	
	public TableInstanceReader(String fileName) {
		this.fileName = fileName;
	}
	
	public void setSchema(boolean schema) {
		this.withSchema = schema;
	}
	
	public TableInstance getTableInstance() {
		if(instance == null) {
			instance = parseFile();
		}
		return instance;
	}
	
	private TableInstance parseFile() {
		List<String> contents = Files.readWholeNoExp(this.fileName);
		if(contents.isEmpty()) {
			throw new Error("The file: " + this.fileName + " should not be empty.");
		}
		String tableName = new File(this.fileName).getName();
		String[] columnNames = null;
		//if without schema, we create some dummy schema names 
		if(!this.withSchema) {
			columnNames = new String[contents.get(0).trim().split(SEP).length];
			//some dummy column names
			for(int i = 0; i < columnNames.length; i++) {
				columnNames[i] = COLUMN_NAME_TBD;
			}
		}
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