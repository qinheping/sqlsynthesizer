package edu.washington.cs.sqlsynth.entity;

import java.util.LinkedList;
import java.util.List;

import checkers.javari.quals.ThisMutable;

import edu.washington.cs.sqlsynth.util.Utils;

/**
 * 
 * @author sunyuyin
 * This calss represents a column, like isMax
 */

public class ComparisionExpr {
	public enum ComparisionType {
		ISMAX { public String toString() { return "isMax"; }},
		ISMIN { public String toString() { return "isMin"; }}
	}
	
	private final TableColumn column;
	
	private ComparisionType t = null;
	
    private TableColumn referColumn = null;
    private Object referColumnValue = null;
	
	public static boolean moreStringOp = false;
	
	public ComparisionExpr(TableColumn column) {
		this.column = column;
	}
	
	public ComparisionExpr(TableColumn column, ComparisionType t) {
		this(column);
		this.t = t;
	}

	public TableColumn getColumn() {
		return column;
	}
	
	public void setReferColumn(TableColumn c, Object columnValue) {
		Utils.checkNotNull(c);
		Utils.checkNotNull(columnValue);
		this.referColumn = c;
		this.referColumnValue = columnValue;
	}
	
	public void setComparisionType(ComparisionType t) {
		this.t = t;
	}
	
	public ComparisionType getT() {
		return t;
	}
	
	public boolean isComplete() {
		return t != null;
	}
	
	public List<ComparisionExpr> enumerateAllExprs() {
		List<ComparisionExpr> completedExprs = new LinkedList<ComparisionExpr>();
		completedExprs.add(new ComparisionExpr(column, ComparisionType.ISMAX));
		completedExprs.add(new ComparisionExpr(column, ComparisionType.ISMIN));
		return completedExprs;
	}
	
	// toSQL function is different
	// TODO
//	public String toSQL() {
//		if (!this.isComplete()) {
//			throw new RuntimeException("The comparision expr is not completed yet.");
//		}
//		return 
//	}
	public String toSQL() {
		if(t == null || this.referColumn == null || this.referColumnValue == null) {
			throw new Error("Not complete");
		}
		StringBuilder sb = new StringBuilder();
		sb.append("(");
		sb.append("select ");
		if(this.equals(ComparisionType.ISMAX)) {
			sb.append("max");
		} else {
			sb.append("min");
		}
		sb.append("(");
		sb.append(this.column.getFullName());
		sb.append(")");
		sb.append(" from ");
		sb.append(this.column.getTableName());
		sb.append(" where ");
		sb.append(this.referColumn.getFullName());
		sb.append("=");
		if(this.referColumn.isIntegerType()) {
			sb.append(this.referColumnValue);
		} else {
			sb.append("'");
			sb.append(this.referColumnValue);
			sb.append("'");
		}
		sb.append(")");
		return sb.toString();
	}
	
	public String toString() {
		return (isComplete() ? this.t.toString() : "NOT-COMPLETE") + "(" + this.column.getFullName() + ")";
	}
}
