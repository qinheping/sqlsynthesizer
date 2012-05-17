package edu.washington.cs.sqlsynth.algorithms;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.washington.cs.sqlsynth.entity.AggregateExpr;
import edu.washington.cs.sqlsynth.entity.TableColumn;
import edu.washington.cs.sqlsynth.entity.TableInstance;
import edu.washington.cs.sqlsynth.util.TableUtils;
import edu.washington.cs.sqlsynth.util.Utils;

public class AggregateExprInfer {

    private final SQLQueryCompletor completor;
	
    private final List<TableInstance> inputTables;
    private final TableInstance outputTable;
    
	public AggregateExprInfer(SQLQueryCompletor completor) {
		this.completor = completor;
		this.inputTables = completor.getInputTables();
		this.outputTable = completor.getOutputTable();
		Utils.checkNotNull(this.inputTables);
		Utils.checkNotNull(this.outputTable);
	}
	
	//it is 0 based
	public Map<Integer, List<AggregateExpr>> inferAggregationExprs() {
		//every column in the output table appears in the input tables
		//suppose there is no sql like:  select a from table group by a
		//such sql can be replaced by :  select distinct a from table;
		if(!this.hasUnmatchedColumns()) {
			return Collections.emptyMap();
		}
		
		List<AggregateExpr> possAggrExprs = this.getUnmatchedColumns();
		
		Map<Integer, List<AggregateExpr>> aggExprs = new LinkedHashMap<Integer, List<AggregateExpr>>();
		List<TableColumn> outputColumns = this.outputTable.getColumns();
		for(int i = 0; i < outputColumns.size(); i++) {
			TableColumn c = outputColumns.get(i);
			TableColumn column = TableUtils.findFirstMatchedColumn(c.getColumnName(), this.inputTables);
			if(column == null) {
				aggExprs.put(i, possAggrExprs);
			}
		}
		return aggExprs;
	}
	
	private List<AggregateExpr> getUnmatchedColumns() {
		List<TableColumn> unmatched = new LinkedList<TableColumn>();
		for(TableInstance t : this.inputTables) {
			for(TableColumn c : t.getColumns()) {
				if(TableUtils.findFirstMatchedColumn(c.getColumnName(), Collections.singletonList(this.outputTable)) == null) {
					unmatched.add(c);
				}
			}
		}
		
		List<AggregateExpr> exprs = new LinkedList<AggregateExpr>();
		for(TableColumn c : unmatched) {
			AggregateExpr e = new AggregateExpr(c);
			exprs.addAll(e.enumerateAllExprs());
		}
		
		return exprs;
		
	}
	
	public List<TableColumn> inferGroupbyColumns() {
		if(!this.hasUnmatchedColumns()) {
			return Collections.emptyList();
		}
		//columns that are not matched
		List<TableColumn> outputColumns = this.outputTable.getColumns();
		List<TableColumn> groupBys = new LinkedList<TableColumn>();
		for(TableColumn c : outputColumns) {
			TableColumn column = TableUtils.findFirstMatchedColumn(c.getColumnName(), this.inputTables);
			if(column != null) { //which one?
				groupBys.add(column);
			}
		}
		Utils.checkTrue(!groupBys.isEmpty());
		return groupBys;
	}
	
	private boolean hasUnmatchedColumns() {
		List<TableColumn> outputColumns = this.outputTable.getColumns();
		for(TableColumn c : outputColumns) {
			TableColumn column = TableUtils.findFirstMatchedColumn(c.getColumnName(), this.inputTables);
			if(column == null) {
				return true;
			}
		}
		return false;
	}
}
