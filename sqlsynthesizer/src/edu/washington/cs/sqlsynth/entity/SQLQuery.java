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
	
	public SQLQuery(SQLSkeleton skeleton) {
		Utils.checkNotNull(skeleton);
		this.skeleton = skeleton;
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
	
	//FIXME definitely not complete yet
	public String toSQLString() {
		StringBuilder sb = new StringBuilder();
		
		sb.append(" select ");
		int count = 0;
		for(int i = 0; i < this.skeleton.getNumOfProjectColumns(); i++) {
			if(count != 0) {
				sb.append(", ");
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
		if(!condition.isEmpty() || this.condition != null) {
		   sb.append(" where ");
    		if(!condition.isEmpty()) {
 	    	    sb.append(skeleton.getAllJoinConditions());
    		}
    		if(!condition.isEmpty() && this.condition != null && !this.condition.isEmpty()) {
    			sb.append(" and ");
    		}
    		if(this.condition != null && !this.condition.isEmpty()) {
    			sb.append(this.condition.toSQLCode());
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
		
		return sb.toString();
	}
}
