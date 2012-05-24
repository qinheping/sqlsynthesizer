package edu.washington.cs.sqlsynth.entity;

import edu.washington.cs.sqlsynth.entity.TableColumn.ColumnType;
import edu.washington.cs.sqlsynth.util.Utils;

/**
 * A wrapper class.
 * */
public class ConditionExpr {

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
		if(this.isTableColumn()) {
			return this.column.isIntegerType();
		} else if (this.isAggregateExpr()) {
			return this.expr.isIntegerType();
		} else {
			throw new Error();
		}
	}
	
	public boolean isStringType() {
		if(this.isTableColumn()) {
			return this.column.isStringType();
		} else if (this.isAggregateExpr()) {
			return this.expr.isStringType();
		} else {
			throw new Error();
		}
	}
	
	public ColumnType getType() {
		if(this.isTableColumn()) {
			return this.column.getType();
		} else if (this.isAggregateExpr()) {
			return this.expr.getColumn().getType();
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
