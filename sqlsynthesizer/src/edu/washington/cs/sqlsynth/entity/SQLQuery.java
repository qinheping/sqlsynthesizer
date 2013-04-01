package edu.washington.cs.sqlsynth.entity;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.washington.cs.sqlsynth.util.Utils;

public class SQLQuery {
	
	private final SQLSkeleton skeleton;
	
	private QueryCondition condition = null;
	
	private Map<Integer, AggregateExpr> aggregateExprs = new LinkedHashMap<Integer, AggregateExpr>();
	
	private List<TableColumn> groupbyColumns = new LinkedList<TableColumn>();
	
	private QueryCondition havingCond = null;
	
	/**
	 * SQL union (SQL Query)
	 * FIXME currently, the union list only contains 1 SQLQuery, see
	 * toSQLString() method.
	 * */
	private List<SQLQuery> unions = new LinkedList<SQLQuery>(); 
	
	private NotExistStmt notExistQuery = null;
	
	public SQLQuery(SQLSkeleton skeleton) {
		Utils.checkNotNull(skeleton);
		this.skeleton = skeleton;
	}
	
	public static SQLQuery clone(SQLQuery q) {
		SQLQuery newQ = new SQLQuery(q.skeleton);
		
		newQ.condition = q.condition;
		newQ.aggregateExprs.putAll(q.aggregateExprs);
		newQ.groupbyColumns.addAll(q.groupbyColumns);
		newQ.havingCond = q.havingCond;
		newQ.unions.addAll(q.unions);
		
		return newQ;
	}
	
	public QueryCondition getCondition() {
		return condition;
	}
	
	public QueryCondition getHavingCond() {
		return this.havingCond;
	}
	
	public void setHavingCondition(QueryCondition cond) {
		Utils.checkNotNull(cond);
		this.havingCond = cond;
	}

	public void setCondition(QueryCondition condition) {
		this.condition = condition;
	}
	
	public void addUnionQuery(SQLQuery q) {
		Utils.checkTrue(q != this);
		this.unions.add(q);
	}

	public Map<Integer, AggregateExpr> getAggregateExprs() {
		return aggregateExprs;
	}

	public void setAggregateExprs(Map<Integer, AggregateExpr> aggregateExprs) {
		this.aggregateExprs = aggregateExprs;
	}

	public List<TableColumn> getGroupbyColumns() {
		return groupbyColumns;
	}

	public void setGroupbyColumns(List<TableColumn> groupbyColumns) {
		this.groupbyColumns = groupbyColumns;
	}

	public SQLSkeleton getSkeleton() {
		return skeleton;
	}
	
	public void setNotExistStmt(NotExistStmt stmt) {
		Utils.checkNotNull(stmt);
		this.notExistQuery = stmt;
	}
	
	public NotExistStmt getNotExistStmt() {
		return this.notExistQuery;
	}
	
	//FIXME definitely not complete yet
	public String toSQLString() {
		StringBuilder sb = new StringBuilder();
		
		sb.append(" select ");
		int count = 0;
		for(int i = 0; i < this.skeleton.getOutputColumnNum(); i++) {
			if(count != 0) {
				sb.append(", ");
			}
			//FIXME
			if(count == 0) {
			    sb.append(" distinct ");
			}
			if(this.skeleton.getProjectColumns().containsKey(i)) {
				sb.append(this.skeleton.getProjectColumns().get(i).getFullName());
			} else if(this.getAggregateExprs().containsKey(i)) {
				sb.append(this.getAggregateExprs().get(i).toSQL());
			} else {
				System.out.println(this.skeleton);
				System.out.println(this.getAggregateExprs());
				Utils.checkTrue(false, "The i: " + i);
			}
			count++;
		}
		
//		for(TableColumn c : this.skeleton.getProjectColumns().values()) {
//			if(count != 0) {
//				sb.append(", ");
//			}
//			sb.append(c.getFullName());
//			count++;
//		}
		sb.append(" from ");
		for(int i = 0; i < skeleton.getTables().size(); i++) {
			if(i!= 0) {
				sb.append(", ");
			}
			sb.append(skeleton.getTables().get(i).getTableName());
		}
		String condition = skeleton.getAllJoinConditions();
		if(!condition.isEmpty() || this.condition != null || this.notExistQuery != null) {
		    
		    StringBuilder cond = new StringBuilder();
    		if(!condition.isEmpty()) {
    			cond.append(skeleton.getAllJoinConditions());
    		}
    		if(!condition.isEmpty() && this.condition != null && !this.condition.isEmpty()) {
    			cond.append(" and ");
    		}
    		if(this.condition != null && !this.condition.isEmpty()) {
    			cond.append(this.condition.toSQLCode());
    		}
    		
    		if(this.notExistQuery != null) {
    			if(condition.isEmpty() && this.condition == null) {
    				//do nothing, do not need an and here
    			} else {
    			    cond.append(" and "); //has condition node above
    			}
    			cond.append(this.notExistQuery.toSQLString());
    		}
    		
    		if(!cond.toString().trim().isEmpty()) {
    		    sb.append(" where ");
    		    sb.append(cond.toString());
    		}
		}
		if(!this.groupbyColumns.isEmpty()) {
			sb.append(" group by ");
			count = 0;
			for(TableColumn c: this.groupbyColumns) {
				if(count != 0) {
					sb.append(", ");
				}
				sb.append(c.getFullName());
			}
		}
		if(this.havingCond != null) {
			sb.append(" having ");
			sb.append(this.havingCond.toSQLCode());
		}
		if(!this.unions.isEmpty()) {
			Utils.checkTrue(this.unions.size() == 1);
			sb.append(" union ");
			sb.append("(");
			SQLQuery append = this.unions.get(0);
			sb.append(append.toSQLString());
//			this.unions.get(0).toSQLString();
			sb.append(")");
		}
		
		return sb.toString();
	}
}
