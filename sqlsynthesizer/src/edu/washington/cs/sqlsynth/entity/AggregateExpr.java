package edu.washington.cs.sqlsynth.entity;

public class AggregateExpr {

	enum AggregateType {SUM, MAX, MIN, AVG};
	
	private final TableColumn column;
	private final AggregateType t;
	
	public  AggregateExpr(TableColumn column, AggregateType t) {
		this.column = column;
		this.t = t;
	}

	public TableColumn getColumn() {
		return column;
	}

	public AggregateType getT() {
		return t;
	}
	
}
