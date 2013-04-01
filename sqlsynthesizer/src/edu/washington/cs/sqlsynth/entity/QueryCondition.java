package edu.washington.cs.sqlsynth.entity;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import plume.Pair;

import edu.washington.cs.sqlsynth.entity.ConditionNode.OP;
import edu.washington.cs.sqlsynth.util.Utils;

public class QueryCondition {
	
	enum CONJ {
		AND { public String toString() { return " and "; }},
		OR { public String toString() { return " or "; }}
		};
	
	/**
	 * The query condition can either be a single root condition node
	 * or a combination of all query conditions.
	 * 
	 * If root is not null, allConds must be null.
	 * */
	private final ConditionNode root;
	
	/**
	 * concatenating the condition node with & or ||
	 * */
	private final boolean isAnd;
	
	/**
	 * A list of query conditions.
	 * 
	 * If allConds is not null, root must be null.
	 * */
	private final List<QueryCondition> allConds;
	
	public QueryCondition(ConditionNode root) {
		Utils.checkNotNull(root);
		this.root = root;
		allConds = null;
		this.isAnd = false;
	}
	
	public static QueryCondition createQueryCondition(List<QueryCondition> allJoins, boolean isAnd) {
		return new QueryCondition(allJoins, isAnd);
	}

	private QueryCondition(ConditionNode root, boolean isAnd, List<QueryCondition> allJoins) {
		//everything not initialized
		this.root = root;
		this.isAnd = isAnd;
		//create a new list
		this.allConds = new LinkedList<QueryCondition>();
		this.allConds.addAll(allJoins);
	}
	
	private QueryCondition(List<QueryCondition> allJoins, boolean isAnd) {
		Utils.checkNotNull(allJoins);
		this.root = null;
		Utils.checkTrue(!allJoins.isEmpty());
		this.isAnd = isAnd;
		//create a new list
		this.allConds = new LinkedList<QueryCondition>();
		this.allConds.addAll(allJoins);
	}
	
	/**
	 * If the root is not null, the condition list must be null.
	 * */
	private boolean isAtom() {
		if(this.root != null) {
			Utils.checkTrue(this.allConds == null);
		}
		return this.root != null;
	}
	
	//Pair<SelectionCondition, QueryCondition>, use null if absent
	//XXX FIXME, the other parts
	//also just use shallow copy
	public Collection<Pair<QueryCondition, QueryCondition>> splitSelectionAndQueryConditions() {
		QueryCondition having = getMostOneAggregation();
		if(having != null) {
			System.out.println(having.toSQLCode());
		}
		QueryCondition other = this;
		if(having != null) {
			other = this.getOtherPart(having);
		}
		
		Collection<Pair<QueryCondition, QueryCondition>> results =
			new LinkedList<Pair<QueryCondition, QueryCondition>>();
		results.add(new Pair<QueryCondition, QueryCondition>(other, having));
		return results;
	}
	
	private QueryCondition getOtherPart(QueryCondition cond) {
		QueryCondition copy = copy(this);
		
//		System.out.println("After copy:  " + copy);
		Utils.checkTrue(!copy.isAtom());
		List<QueryCondition> list = new LinkedList<QueryCondition>();
		list.addAll(copy.allConds);
		QueryCondition firstLevelToRemove = null;
	    for(QueryCondition q : copy.allConds) {
	    	if(q.isAtom() && q.root.isLeaf() && q.root.getLeftExpr().isAggregateExpr()) {
	    		firstLevelToRemove = q;
	    		break;
	    	}
	    }
	    if(firstLevelToRemove != null) {
	    	System.out.println("removing: " + firstLevelToRemove);
	    	copy.allConds.remove(firstLevelToRemove);
	    	return copy;
	    }
		
		
		Set<QueryCondition> visited = new HashSet<QueryCondition>();
		while(!list.isEmpty()) {
			QueryCondition v = list.remove(0);
			if(visited.contains(v)) {
				continue;
			}
			visited.add(v);
			if(!v.isAtom()) {
				QueryCondition toRemove = null;
			    for(QueryCondition q : v.allConds) {
			    	if(q.isAtom() && q.root.isLeaf() && q.root.getLeftExpr().isAggregateExpr()) {
			    		toRemove = q;
			    		break;
			    	}
			    }
			    if(toRemove != null) {
			    	System.out.println("removing: " + toRemove);
			    	v.allConds.remove(toRemove);
			    	break;
			    }
			}
			if(!v.isAtom()) {
				list.addAll(v.allConds);
			}
		}
		
		return copy;
	}
	
	private QueryCondition getMostOneAggregation() {
		Utils.checkTrue(!this.isAtom());
		QueryCondition aggQuery = null;
		List<QueryCondition> list = new LinkedList<QueryCondition>();
		list.addAll(this.allConds);
		
		Set<QueryCondition> visited = new HashSet<QueryCondition>();
		int count = 0;
		while(!list.isEmpty()) {
			QueryCondition v = list.remove(0);
			if(visited.contains(v)) {
				continue;
			}
			visited.add(v);
			if(v.isAtom() && v.root.isLeaf() && v.root.getLeftExpr().isAggregateExpr()) {
				count++;
//				System.out.println(v.toSQLCode());
				aggQuery = v;
				continue;
			}
			if(!v.isAtom()) {
				list.addAll(v.allConds);
			}
		}
		
		System.out.println("Number of expression: " + count);
		Utils.checkTrue(count < 2);
		return aggQuery;
	}
	
	public boolean isEmpty() {
		String sql = this.toSQLCode();
		return sql.equals("()");
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
	
	@Override
	public String toString() {
		return this.toSQLCode();
	}
	
	/**************************************
	 * Below are the helper static methods
	 **************************************/
	
	/**
	 * Reverse the query condition by returning a new query condition object.
	 * */
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
	
	public static QueryCondition parse(Map<String, TableColumn> columnMap, String cond) {
		return parse(columnMap, new HashMap<String, AggregateExpr>(), cond);
	}
	
	public static QueryCondition parse(Map<String, TableColumn> columnMap, Map<String, AggregateExpr> aggMap, String cond) {
		if(cond.isEmpty()) {
			return null;
		}
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
		QueryCondition q = parseInternal(columnMap, aggMap, cond);
		
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
	private static QueryCondition parseInternal(Map<String, TableColumn> columnMap,
			Map<String, AggregateExpr> aggMap, String cond) {
		//return null;
		String AND = "AND";
		String[] rules = splitAtTopLevel(cond, AND);
		List<QueryCondition> conditions = new LinkedList<QueryCondition>();
		for(String rule : rules) {
			if(rule.trim().isEmpty()) {
				continue;
			}
			if(rule.indexOf(AND) == -1) {
				ConditionNode node = parseNode(columnMap, aggMap, rule);
				QueryCondition q = new QueryCondition(node);
				conditions.add(q);
			} else {
				cond = eliminateMatchedPara(cond);
				//((NOT (ID_key_room_count > 1.0)) AND (NOT (ID_key_room_count <= 2.0))) AND (NOT (room = R128))
				conditions.add(parse(columnMap, aggMap, rule));
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
	
	public static ConditionNode  parseNode(Map<String, TableColumn> columnMap,String predicate) {
		return parseNode(columnMap, new HashMap<String, AggregateExpr>(), predicate);
	}
	
	public static ConditionNode  parseNode(Map<String, TableColumn> columnMap, Map<String, AggregateExpr> aggMap,
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
		ConditionNode node = parseNodeInternal(columnMap, aggMap, predicate);
		if(needReverse) {
//			System.out.println("reverse a single node: " + node.toSQLString());
			node = ConditionNode.reverseOp(node);
//			System.out.println("after reverse a single node: " + node.toSQLString());
		}
		return node;
	}
	
	private static ConditionNode  parseNodeInternal(Map<String, TableColumn> columnMap,
			Map<String, AggregateExpr> aggMap, String predicate) {
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
		AggregateExpr leftAgg = aggMap.get(leftPart);
		Utils.checkTrue(leftColumn != null || leftAgg != null, "Not exist? " + leftPart); //FIXME not accurate
		Utils.checkTrue(leftColumn == null || leftAgg == null, "All exist? " + leftPart); //FIXME not accurate
		ConditionExpr leftExpr = leftColumn != null ? new ConditionExpr(leftColumn) : new ConditionExpr(leftAgg);
		
		TableColumn rightColumn = columnMap.get(rightPart);
		if(rightColumn == null) {
			if(leftExpr.isIntegerType()) {
				Integer t = Utils.convertToInteger(rightPart);
//				return ConditionNode.createInstance(op, leftColumn, null, t);
				return new ConditionNode(op, leftExpr, null, t);
			}
//			return ConditionNode.createInstance(op, leftColumn, null, rightPart);
			return new ConditionNode(op, leftExpr, null, rightPart);
		} else {
			ConditionExpr rightExpr = new ConditionExpr(rightColumn);
			return new ConditionNode(op, leftExpr, rightExpr, null);
		}
	}
}
