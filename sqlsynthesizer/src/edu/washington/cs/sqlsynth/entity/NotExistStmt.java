package edu.washington.cs.sqlsynth.entity;

import edu.washington.cs.sqlsynth.util.Utils;

//it belongs to the condition node
public class NotExistStmt {

	public final SQLQuery query;
	
	public NotExistStmt(SQLQuery query) {
		Utils.checkNotNull(query);
		this.query = query;
	}
	
	public String toSQLString() {
		StringBuilder sb = new StringBuilder();
		sb.append("NOT EXISTS (");
		sb.append(query.toSQLString());
		sb.append(")");
		return sb.toString();
	}
}
