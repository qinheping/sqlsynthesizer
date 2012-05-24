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
	
	private final boolean isAnd;
	
	private final List<QueryCondition> allConds;
	
	public QueryCondition(ConditionNode root) {
		Utils.checkNotNull(root);
		this.root = root;
		allConds = null;
		this.isAnd = false;
	}

	private QueryCondition(ConditionNode root, boolean isAnd, List<QueryCondition> allJoins) {
		//everything not initialized
		this.root = root;
		this.isAnd = isAnd;
		this.allConds = allJoins;
	}
	
	private QueryCondition(List<QueryCondition> allJoins, boolean isAnd) {
		Utils.checkNotNull(allJoins);
		this.root = null;
		Utils.checkTrue(!allJoins.isEmpty());
		this.isAnd = isAnd;
		this.allConds = allJoins;
	}
	
	public static QueryCondition reverse(QueryCondition cond) {
		if(cond.isAtom()) {
			QueryCondition revQuery = copy(cond);
			revQuery.root.reverseOp();
			return revQuery;
		} else {
			//change isAnd, as well as reverse the list of queries
			//reverse the isAnd flag
			QueryCondition revQuery = new QueryCondition(cond.allConds, !cond.isAnd);
			//reverse the query condition
			List<QueryCondition> revQs = new LinkedList<QueryCondition>();
			for(QueryCondition q : cond.allConds) {
				revQs.add(reverse(q));
			}
			cond.allConds.clear();
			cond.allConds.addAll(revQs);
			return revQuery;
		}
	}
	
	private boolean isAtom() {
		if(this.root != null) {
			Utils.checkTrue(this.allConds == null);
		}
		return this.root != null;
	}
	
	public static QueryCondition copy(QueryCondition cond) {
		boolean isAtom = cond.isAtom();
		List<QueryCondition> list = isAtom ? null : new LinkedList<QueryCondition>();
		QueryCondition c = new QueryCondition(cond.root, cond.isAnd, list);
		if(isAtom) {
			return c;
		} else {
			for(QueryCondition q : cond.allConds) {
				c.allConds.add(QueryCondition.copy(q));
			}
			return c;
		}
	}
	
	
	public String toSQLCode() {
		if(isAtom()) {
			Utils.checkTrue(this.allConds == null);
		    return root.toSQLString();
		} else {
			Utils.checkTrue(this.allConds != null);
			List<QueryCondition> joins = this.allConds;
			String symbol = this.isAnd ? " and " : " or ";
			StringBuilder sb = new StringBuilder();
			sb.append("(");
			for(int i = 0; i < joins.size(); i++) {
				if(i != 0) {
					sb.append(symbol);
				}
				sb.append(joins.get(i).toSQLCode()); //to SQL condi
			}
			sb.append(")");
			return sb.toString();
		}
	}
	
	/**
	 * Below are the helper static methods
	 * */
	public static QueryCondition parse(Map<String, TableColumn> columnMap, String cond) {
		cond = eliminateMatchedPara(cond);
		boolean needReverse = false;
		String NOT = "NOT";
		//need to reverse, such as (NOT (a > b and a < c)), so need to eliminate "(", ")" twice
		if(cond.startsWith(NOT)) {
			needReverse = true;
			cond = cond.substring(NOT.length());
			cond = cond.trim();
		}
		cond = cond.trim();
		System.out.println("parse query condition: " + cond);
		cond = eliminateMatchedPara(cond);
		QueryCondition q = parseInternal(columnMap, cond);
		
		if(needReverse) {
			q = reverse(q);
		}
		return q;
	}
	
	static String eliminateMatchedPara(String cond) {
		String newCond = cond.trim();
		while(newCond.startsWith("(") && newCond.endsWith(")") && canEliminate(newCond)) {
			newCond = newCond.substring(1, newCond.length() - 1);
			newCond = newCond.trim();
		}
		return newCond;
	}
	
	private static boolean canEliminate(String cond) {
		int count = 0;
		for(int i = 0; i < cond.length(); i++) {
			char c = cond.charAt(i);
			if( c == '(') {
				count++;
			} else if (c == ')') {
				count--;
			}
			if(count == 0 && i != cond.length() - 1) {
				return false;
			}
		}
		return true;
	}
	
	private static boolean paraMatched(String cond) {
		int count = 0;
		for(int i = 0; i < cond.length(); i++) {
			char c = cond.charAt(i);
			if( c == '(') {
				count++;
			} else if (c == ')') {
				count--;
			}
		}
		return count == 0;
	}
	
	//ID_key_ID_key_student_count <= 4.0 AND Room != R128
	//what about this one: ((NOT (C1)) AND (NOT (C2))) AND (C3)? XXXFIXME
	private static QueryCondition parseInternal(Map<String, TableColumn> columnMap, String cond) {
		//return null;
		String AND = "AND";
		String[] rules = splitAtTopLevel(cond, AND);
		List<QueryCondition> conditions = new LinkedList<QueryCondition>();
		for(String rule : rules) {
			if(rule.trim().isEmpty()) {
				continue;
			}
			if(rule.indexOf(AND) == -1) {
				ConditionNode node = parseNode(columnMap, rule);
				QueryCondition q = new QueryCondition(node);
				conditions.add(q);
			} else {
				cond = eliminateMatchedPara(cond);
				//((NOT (ID_key_room_count > 1.0)) AND (NOT (ID_key_room_count <= 2.0))) AND (NOT (room = R128))
				conditions.add(parse(columnMap, rule));
			}
		}
		Utils.checkTrue(!conditions.isEmpty());
		
		return new QueryCondition(conditions, true);
	}
	
	static String[] splitAtTopLevel(String cond, String symbol) {
		Utils.checkTrue(!canEliminate(cond), "Error: " + cond);
		String[] splits = cond.split(symbol);
		
		List<String> results = new LinkedList<String>();
		
		String unmatched = null;
		for(String split : splits) {
			if(paraMatched(split)) {
				results.add(split);
			} else {
				if(unmatched == null) {
					unmatched = split;
				} else {
					unmatched = unmatched + symbol + split;
				}
				if(paraMatched(unmatched)) {
					results.add(unmatched);
					unmatched = null;
				}
			}
		}
		//System.out.println(unmatched);
		Utils.checkTrue(unmatched == null, unmatched);
		
		return results.toArray(new String[0]);
	}
	
	public static ConditionNode  parseNode(Map<String, TableColumn> columnMap,
			String predicate) {
		//(NOT (a > b))
		predicate = eliminateMatchedPara(predicate);
		boolean needReverse = false;
		String NOT = "NOT";
		//need to reverse, such as (NOT (a > b and a < c)), so need to eliminate "(", ")" twice
		System.out.println("reverse: " + predicate);
		if(predicate.startsWith(NOT)) {
			needReverse = true;
			predicate = predicate.substring(NOT.length());
			predicate = predicate.trim();
		}
		predicate = eliminateMatchedPara(predicate);
		ConditionNode node = parseNodeInternal(columnMap, predicate);
		if(needReverse) {
//			System.out.println("reverse a single node: " + node.toSQLString());
			node = ConditionNode.reverseOp(node);
//			System.out.println("after reverse a single node: " + node.toSQLString());
		}
		return node;
	}
	
	private static ConditionNode  parseNodeInternal(Map<String, TableColumn> columnMap,
			String predicate) {
		System.out.println("parseNodeInternal: predicate: " + predicate);
		Utils.checkTrue(predicate != null);
		Utils.checkTrue(!predicate.isEmpty());
		String[] symbols = new String[]{
				OP.NE.toString().trim(), OP.LE.toString().trim(),
				OP.EQ.toString().trim() /*= must be after <=*/,OP.GT.toString().trim()}; 
		String matched = null;
		for(String symbol : symbols) {
			if(predicate.indexOf(symbol) != -1) {
				matched = symbol;
				break;
			}
		}
		Utils.checkTrue(matched != null);
		
		System.out.println("before split:  " + predicate);
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
				return ConditionNode.createInstance(op, leftColumn, null, t);
			}
			return ConditionNode.createInstance(op, leftColumn, null, rightPart);
		} else {
			return ConditionNode.createInstance(op, leftColumn, rightColumn, null);
		}
	}
	
//	public static QueryCondition constructAllAndQueryCondition(List<ConditionNode> nodes) {
//		List<QueryCondition> conds = new LinkedList<QueryCondition>();
//		for(ConditionNode node : nodes) {
//			conds.add(new QueryCondition(node));
//		}
//		return new QueryCondition(conds, true);
//	}
	
//	public static QueryCondition constructAllOrQueryCondition(List<ConditionNode> nodes) {
//		List<QueryCondition> conds = new LinkedList<QueryCondition>();
//		for(ConditionNode node : nodes) {
//			conds.add(new QueryCondition(node));
//		}
//		return new QueryCondition(conds, false);
//	}
}
