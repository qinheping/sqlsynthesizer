package edu.washington.cs.sqlsynth.entity;

import edu.washington.cs.sqlsynth.entity.QueryCondition.CONJ;
import edu.washington.cs.sqlsynth.util.Utils;

/**
 * This class represents a single or compound predicate.
 * 
 * A single predicate looks like:  a > b. To represent such a predicate, this
 * ConditionNode is a leaf node. In this case, ifLeaf = true, op = GT,
 * leftColumn = a, rightColumn = b, righConstant = null.  conj = null,
 * left = right = null.
 * 
 * Note that, to represent the comparison with a constant, the constant
 * must be put on the right hand side (the rightContant field). For example,
 * a predicate: 5 > a should be represented as a < 5 (move the constant to the right)  
 * 
 * 
 * 
 * A compound predicate looks like:  a > b  or  c > 10. To represent such
 * a predicate, the ConditionNode is NOT a leaf node. It will have two
 * children (left, right) fields.  A rough representation looks like:
 *               or
 *             /   \
 *            /     \
 *          a > b   c > 10
 * In the above case, isLeaf = false, op = null, leftColumn= rightColumn = rightConstant = null
 * conj = OR, left = a > b (a leaf ConditionNode), right = c > 10 (a leaf COnditionNode)
 * */
public class ConditionNode {
	
	enum OP{
		/*modifiy it as the same as weka*/
		GT { public String toString() { return " > "; }},
		NE { public String toString() { return " != "; }},
		EQ { public String toString() { return " = "; }},
		LE { public String toString() { return " <= "; }}
		}; //greater than, equal, or less than

	public static OP getOP(String str) {
		Utils.checkNotNull(str);
		String trim = str.trim();
		if(trim.equals(OP.GT.toString().trim())) {
			return OP.GT;
		} else if (trim.equals(OP.NE.toString().trim())) {
			return OP.NE;
		} else if (trim.equals(OP.EQ.toString().trim())) {
			return OP.EQ;
		} else if (trim.equals(OP.LE.toString().trim())) {
			return OP.LE;
		} else {
			return null;
		}
	}
	
	public static OP getOppositeOP(String str) {
		OP op = getOP(str);
		return getOppositeOP(op);
	}
	
    public static OP getOppositeOP(OP op) {
		if(op.equals(OP.GT)) {
			return OP.LE;
		}
		if(op.equals(OP.LE)) {
			return OP.GT;
		}
		if(op.equals(OP.EQ)) {
			return OP.NE;
		}
		if(op.equals(OP.NE)) {
			return OP.EQ;
		}
		throw new Error("invalid op: " + op);
	}
    
    public static ConditionNode reverseOp(ConditionNode node) {
    	ConditionNode revNode = copy(node);
    	revNode.reverseOp();
//    	revNode.setOp(getOppositeOP(revNode.getOp()));
    	return revNode;
    }
    
    public static ConditionNode copy(ConditionNode node) {
    	if(node.isLeaf) {
    		return new ConditionNode(node.op, node.leftExpr, node.rightExpr, node.rightConstant);
    	} else {
    		return new ConditionNode(node.conj, node.leftNode, node.rightNode);
    	}
    }
		
	private final boolean isLeaf;
	
	//if it is a condition node, i.e., a leaf node
	private OP op = null;
	private ConditionExpr leftExpr = null;
	private ConditionExpr rightExpr = null;
	private Object rightConstant = null; //constant is always on the right side
	
	//if it is not a condition node, i.e., a non-leaf node
	private CONJ conj = null; //"and" or "or"
	private ConditionNode leftNode = null;
	private ConditionNode rightNode = null;
	
	public static ConditionNode createInstance(OP op, AggregateExpr leftAgg, TableColumn rightColumn, Object rightConstant) {
		ConditionExpr rightExpr = rightColumn == null ? null : new ConditionExpr(rightColumn);
		return new ConditionNode(op, new ConditionExpr(leftAgg), rightExpr, rightConstant);
	}
	
	//create a leaf node
	public static ConditionNode createInstance(OP op, TableColumn leftColumn, TableColumn rightColumn, Object rightConstant) {
		ConditionExpr rightExpr = rightColumn == null ? null : new ConditionExpr(rightColumn);
		return new ConditionNode(op, new ConditionExpr(leftColumn), rightExpr, rightConstant);
	}
	
	public ConditionNode(OP op, ConditionExpr leftExpr, ConditionExpr rightExpr, Object rightConstant) {
		this.isLeaf = true;
		Utils.checkNotNull(op);
		Utils.checkNotNull(leftExpr);
		if(rightExpr == null) {
			Utils.checkNotNull(rightConstant);
			//check type compatibility
			if(leftExpr.isIntegerType()) {
				Utils.checkTrue(Utils.isInteger(rightConstant+""));
			}
		} else {
			Utils.checkTrue(rightConstant == null);
			Utils.checkTrue(leftExpr.getType().equals(leftExpr.getType()));
		}
		//check the type
		if(op.equals(OP.GT) || op.equals(OP.LE)) {
			Utils.checkTrue(leftExpr.isIntegerType(), "Only integer can be compared using < and > .");
			if(rightExpr != null) {
				Utils.checkTrue(rightExpr.isIntegerType(), "Only integer can be compared using < and > .");
			} else {
				Utils.checkTrue(Utils.isInteger(rightConstant+""), "Only integer can be compared using < and > .");
			}
		}
		this.op = op;
		this.leftExpr = leftExpr;
		this.rightExpr = rightExpr;
		this.rightConstant = rightConstant;
	}
	
	//create a null leaf node
	public ConditionNode(CONJ conj, ConditionNode leftNode, ConditionNode rightNode) {
		this.isLeaf = false;
		Utils.checkNotNull(conj);
		Utils.checkNotNull(leftNode);
		Utils.checkNotNull(rightNode);
		this.conj = conj;
		this.leftNode = leftNode;
		this.rightNode = rightNode;
	}

	public OP getOp() {
		return op;
	}

	public void setOp(OP op) {
		Utils.checkNotNull(op);
		this.op = op;
	}
	
	public void reverseOp() {
		this.setOp(getOppositeOP(this.op));
	}

	public ConditionExpr getLeftColumn() {
		return this.leftExpr;
	}

	public void setLeftExpr(ConditionExpr leftExpr) {
		Utils.checkNotNull(leftExpr);
		this.leftExpr = leftExpr;
	}

	public ConditionExpr getRightExpr() {
		return this.rightExpr;
	}

	public void setRightExpr(ConditionExpr rightExpr) {
		this.rightExpr = rightExpr;
	}

	public Object getRightConstant() {
		return rightConstant;
	}

	public void setRightConstant(Object rightConstant) {
		this.rightConstant = rightConstant;
	}

	public CONJ getConj() {
		return conj;
	}

	public void setConj(CONJ conj) {
		Utils.checkNotNull(conj);
		this.conj = conj;
	}

	public ConditionNode getLeftNode() {
		return leftNode;
	}

	public void setLeftNode(ConditionNode leftNode) {
		Utils.checkNotNull(leftNode);
		this.leftNode = leftNode;
	}

	public ConditionNode getRightNode() {
		return rightNode;
	}

	public void setRightNode(ConditionNode rightNode) {
		Utils.checkNotNull(rightNode);
		this.rightNode = rightNode;
	}

	public boolean isLeaf() {
		return isLeaf;
	}
	
	//check all invariant of this class must hold
	private void repOK() {
		if(this.isLeaf) {
			Utils.checkNotNull(leftExpr);
			Utils.checkNotNull(this.op);
			if(rightExpr == null) {
				Utils.checkNotNull(rightConstant);
			} else {
				Utils.checkTrue(rightConstant == null);
			}
		} else {
			Utils.checkNotNull(conj);
			Utils.checkNotNull(leftNode);
			Utils.checkNotNull(rightNode);
		}
		// check the type
		if (op != null) {
			if (op.equals(OP.GT) || op.equals(OP.LE)) {
				Utils.checkTrue(leftExpr.isIntegerType(),
						"Only integer can be compared using < and > .");
				if (rightExpr != null) {
					Utils.checkTrue(rightExpr.isIntegerType(),
							"Only integer can be compared using < and > .");
				} else {
					Utils.checkTrue(Utils.isInteger(rightConstant + ""),
							"Only integer can be compared using < and > .");
				}
			}
		}
	}
	
	public String toSQLString() {
		this.repOK();
		if(this.isLeaf) {
			String leftPart = this.leftExpr.toSQLCode();
			String opStr = this.op.toString();
			String rightPart = this.rightExpr == null ? this.rightConstant.toString() : this.rightExpr.toSQLCode();
			String leftPara = (this.leftExpr.isStringType() && this.rightExpr == null) ? "\'" : "";
			String rightPara = (this.leftExpr.isStringType() && this.rightExpr == null) ? "\'" : "";
			return leftPart + opStr + leftPara+ rightPart + rightPara;
//			return leftPart + opStr+ rightPart;
		} else {
			String leftPart = "(" + this.leftNode.toSQLString() + ")";
			String opStr = this.conj.toString();
			String rightPart = "(" + this.rightNode.toSQLString() + ")";
			return leftPart + opStr + rightPart;
		}
	}
	
	/**
	 * Feel free to add other methods you need to initialize, manipulate, and update this object
	 * */
}