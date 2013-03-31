package edu.washington.cs.sqlsynth.entity;

import edu.washington.cs.sqlsynth.entity.AggregateExpr.AggregateType;
import edu.washington.cs.sqlsynth.entity.TableColumn.ColumnType;
import edu.washington.cs.sqlsynth.util.Utils;

/**
 * A wrapper class, indicating that a condition expression can
 * either be an aggregate expression like sum(column_name) or
 * just a column name.
 * */
public class ConditionExpr {

	/**
	 * Only one of them can be non-null
	 * */
	public final AggregateExpr expr;
	public final TableColumn column;
	
	public ConditionExpr(AggregateExpr expr) {
		Utils.checkNotNull(expr);
		this.expr = expr;
		this.column = null;
	}
	
	public ConditionExpr(TableColumn column) {
		Utils.checkNotNull(column);
		this.expr = null;
		this.column = column;
	}
	
	public boolean isTableColumn() {
		boolean yes = column != null;
		if(yes) {
			Utils.checkTrue(this.expr == null);
		}
		return yes;
	}
	
	public boolean isAggregateExpr() {
		boolean yes = expr != null;
		if(yes) {
			Utils.checkTrue(this.column == null);
		}
		return yes;
	}
	
	public boolean isIntegerType() {
		return this.getType().equals(ColumnType.Integer);
	}
	
	public boolean isStringType() {
		return this.getType().equals(ColumnType.String);
	}
	
	public ColumnType getType() {
		if(this.isTableColumn()) {
			return this.column.getType();
		} else if (this.isAggregateExpr()) {
			if(expr.getT().equals(AggregateType.COUNT)) { //no matter which one, always return int
				return ColumnType.Integer;
			} else {
			    return this.expr.getColumn().getType();
			}
		} else {
			throw new Error();
		}
	}
	
	public String toSQLCode() {
		if(this.isTableColumn()) {
			return this.column.getFullName();
		} else if (this.isAggregateExpr()) {
			return this.expr.toSQL();
		} else {
			throw new Error();
		}
	}
}
