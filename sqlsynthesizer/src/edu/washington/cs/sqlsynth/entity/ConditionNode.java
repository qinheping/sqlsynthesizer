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
    		return new ConditionNode(node.op, node.leftColumn, node.rightColumn, node.rightConstant);
    	} else {
    		return new ConditionNode(node.conj, node.leftNode, node.rightNode);
    	}
    }
		
	private final boolean isLeaf;
	
	//if it is a condition node, i.e., a leaf node
	private OP op = null;
	private TableColumn leftColumn = null;
	private TableColumn rightColumn = null;
	private Object rightConstant = null; //constant is always on the right side
	
	//if it is not a condition node, i.e., a non-leaf node
	private CONJ conj = null; //"and" or "or"
	private ConditionNode leftNode = null;
	private ConditionNode rightNode = null;
		
	//create a leaf node
	public ConditionNode(OP op, TableColumn leftColumn, TableColumn rightColumn, Object rightConstant) {
		this.isLeaf = true;
		Utils.checkNotNull(op);
		Utils.checkNotNull(leftColumn);
		if(rightColumn == null) {
			Utils.checkNotNull(rightConstant);
			//check type compatibility
			if(leftColumn.isIntegerType()) {
				Utils.checkTrue(Utils.isInteger(rightConstant+""));
			}
		} else {
			Utils.checkTrue(rightConstant == null);
			Utils.checkTrue(leftColumn.getType().equals(rightColumn.getType()));
		}
		//check the type
		if(op.equals(OP.GT) || op.equals(OP.LE)) {
			Utils.checkTrue(leftColumn.isIntegerType(), "Only integer can be compared using < and > .");
			if(rightColumn != null) {
				Utils.checkTrue(rightColumn.isIntegerType(), "Only integer can be compared using < and > .");
			} else {
				Utils.checkTrue(Utils.isInteger(rightConstant+""), "Only integer can be compared using < and > .");
			}
		}
		this.op = op;
		this.leftColumn = leftColumn;
		this.rightColumn = rightColumn;
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

	public TableColumn getLeftColumn() {
		return leftColumn;
	}

	public void setLeftColumn(TableColumn leftColumn) {
		Utils.checkNotNull(leftColumn);
		this.leftColumn = leftColumn;
	}

	public TableColumn getRightColumn() {
		return rightColumn;
	}

	public void setRightColumn(TableColumn rightColumn) {
		this.rightColumn = rightColumn;
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
			Utils.checkNotNull(leftColumn);
			Utils.checkNotNull(this.op);
			if(rightColumn == null) {
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
				Utils.checkTrue(leftColumn.isIntegerType(),
						"Only integer can be compared using < and > .");
				if (rightColumn != null) {
					Utils.checkTrue(rightColumn.isIntegerType(),
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
			String leftPart = this.leftColumn.getFullName();
			String opStr = this.op.toString();
			String rightPart = this.rightColumn == null ? this.rightConstant.toString() : this.rightColumn.getFullName();
			String leftPara = (this.leftColumn.isStringType() && this.rightColumn == null) ? "\'" : "";
			String rightPara = (this.leftColumn.isStringType() && this.rightColumn == null) ? "\'" : "";
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