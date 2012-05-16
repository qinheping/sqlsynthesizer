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
	
	public SQLQuery(SQLSkeleton skeleton) {
		Utils.checkNotNull(skeleton);
		this.skeleton = skeleton;
	}
	
	public QueryCondition getCondition() {
		return condition;
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
		for(TableColumn c : this.skeleton.getProjectColumns().values()) {
			if(count != 0) {
				sb.append(", ");
			}
			sb.append(c.getFullName());
			count++;
		}
		sb.append(" from ");
		for(int i = 0; i < skeleton.getTables().size(); i++) {
			if(i!= 0) {
				sb.append(", ");
			}
			sb.append(skeleton.getTables().get(i).getTableName());
		}
		sb.append(" where ");
		//wrong here FIXME
		sb.append(skeleton.getAllJoinConditions());
		
		return sb.toString();
	}
}
