package edu.washington.cs.sqlsynth.entity;

import java.util.LinkedList;
import java.util.List;

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
	
	public String toString() {
		return (isComplete() ? this.t.toString() : "NOT-COMPLETE") + "(" + this.column.getFullName() + ")";
	}
}
