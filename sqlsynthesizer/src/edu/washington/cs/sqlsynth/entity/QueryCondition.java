package edu.washington.cs.sqlsynth.entity;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.washington.cs.sqlsynth.entity.ConditionNode.OP;
import edu.washington.cs.sqlsynth.util.Utils;

public class QueryCondition {
	
	enum CONJ {
		AND { public String toString() { return " and "; }},
		OR { public String toString() { return " or "; }}
		};
	
	private final ConditionNode root;
	
	private final List<ConditionNode> allJoins;
	
	public QueryCondition(ConditionNode root) {
		Utils.checkNotNull(root);
		this.root = root;
		allJoins = null;
	}
	
	public QueryCondition(List<ConditionNode> allJoins) {
		Utils.checkNotNull(allJoins);
		this.root = null;
		Utils.checkTrue(!allJoins.isEmpty());
		this.allJoins = allJoins;
	}
	
	public String toSQLCode() {
		if(this.root != null) {
			Utils.checkTrue(this.allJoins == null);
		    return root.toSQLString();
		} else {
			Utils.checkNotNull(this.allJoins);
			StringBuilder sb = new StringBuilder();
			sb.append("(");
			for(int i = 0; i < this.allJoins.size(); i++) {
				if(i != 0) {
					sb.append(" and ");
				}
				sb.append(this.allJoins.get(i).toSQLString());
			}
			sb.append(")");
			return sb.toString();
		}
	}
	
	//ID_key_ID_key_student_count <= 4.0 AND Room != R128
	public static QueryCondition parse(Map<String, TableColumn> columnMap, String cond) {
		//return null;
		String AND = "AND";
		String[] rules = cond.split(AND);
		List<ConditionNode> nodes = new LinkedList<ConditionNode>();
		for(String rule : rules) {
			if(rule.trim().isEmpty()) {
				continue;
			}
			ConditionNode node = parseNode(columnMap, rule);
			nodes.add(node);
		}
		Utils.checkTrue(!nodes.isEmpty());
		
		return constructQueryCondition(nodes);
	}
	
	public static ConditionNode  parseNode(Map<String, TableColumn> columnMap,
			String predicate) {
		Utils.checkTrue(predicate != null);
		Utils.checkTrue(!predicate.isEmpty());
		String[] symbols = new String[]{
				OP.NE.toString().trim(), OP.LE.toString().trim(),
				OP.EQ.toString().trim() /*= must be behind <=*/,OP.GT.toString().trim()}; 
		String matched = null;
		for(String symbol : symbols) {
			if(predicate.indexOf(symbol) != -1) {
				matched = symbol;
				break;
			}
		}
		Utils.checkTrue(matched != null);
		
		String[] splits = predicate.split(matched);
		Utils.checkTrue(splits.length == 2);
		
		String leftPart = splits[0].trim();
		String rightPart = splits[1].trim();
		OP op = ConditionNode.getOP(matched);
		Utils.checkNotNull(op);
		
		TableColumn leftColumn = columnMap.get(leftPart);
		Utils.checkTrue(leftColumn != null, "Not exist? " + leftPart); //FIXME not accurate
		TableColumn rightColumn = columnMap.get(rightPart);
		if(rightColumn == null) {
			if(leftColumn.isIntegerType()) {
				Integer t = Utils.convertToInteger(rightPart);
				return new ConditionNode(op, leftColumn, null, t);
			}
			return new ConditionNode(op, leftColumn, null, rightPart);
		} else {
			return new ConditionNode(op, leftColumn, rightColumn, null);
		}
	}
	
	public static QueryCondition constructQueryCondition(List<ConditionNode> nodes) {
		return new QueryCondition(nodes);
	}
}
