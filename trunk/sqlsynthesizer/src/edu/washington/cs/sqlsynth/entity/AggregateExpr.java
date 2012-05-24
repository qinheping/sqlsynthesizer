package edu.washington.cs.sqlsynth.entity;

import java.util.LinkedList;
import java.util.List;

public class AggregateExpr {
	public enum AggregateType {
        COUNT { public String toString() { return "count"; }},
		SUM { public String toString() { return "sum"; }},
		MAX { public String toString() { return "max"; }},
		MIN { public String toString() { return "min"; }},
		AVG { public String toString() { return "avg"; }}
	}
	
	private final TableColumn column;
	
	private AggregateType t = null;
	
	public static boolean moreStringOp = false;
	
	public  AggregateExpr(TableColumn column) {
		this.column = column;
	}
	
	public  AggregateExpr(TableColumn column, AggregateType t) {
		this(column);
		this.t = t;
	}

	public TableColumn getColumn() {
		return column;
	}
	
	public boolean isIntegerType() {
		return this.column.isIntegerType();
	}
	
	public boolean isStringType() {
		return this.column.isStringType();
	}
	
	public void setAggregateType(AggregateType t) {
		this.t = t;
	}

	public AggregateType getT() {
		return t;
	}
	
	public boolean isComplete() {
		return t != null;
	}
	
	public List<AggregateExpr> enumerateAllExprs() {
		List<AggregateExpr> completedExprs = new LinkedList<AggregateExpr>();
		completedExprs.add(new AggregateExpr(column, AggregateType.COUNT));
		if(this.column.isIntegerType()) {
		    completedExprs.add(new AggregateExpr(column, AggregateType.SUM));
		    completedExprs.add(new AggregateExpr(column, AggregateType.AVG));
		    completedExprs.add(new AggregateExpr(column, AggregateType.MAX));
		    completedExprs.add(new AggregateExpr(column, AggregateType.MIN));
		} else if (this.column.isStringType() && moreStringOp) { //no average, and sum
		    completedExprs.add(new AggregateExpr(column, AggregateType.MAX));
		    completedExprs.add(new AggregateExpr(column, AggregateType.MIN));
		}
		return completedExprs;
	}
	
	public String toSQL() {
		if(!this.isComplete()) {
			throw new RuntimeException("The aggregate expr is not completed yet.");
		}
		return this.t.toString() + "(" + this.column.getFullName() + ")";
	}
	
	@Override
	public String toString() {
		return (isComplete() ? this.t.toString() : "NOT-COMPLETE") + "(" + this.column.getFullName() + ")";
	}
}
