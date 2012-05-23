package edu.washington.cs.sqlsynth.entity;




import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestBTree extends TestCase {

	public static Test suite() {
		return new TestSuite(TestBTree.class);
	}
	
	public void testBuildTreeFromRules1() {
		
		String[] allRules = {"ID_key_ID_key_student_count <= 4.0 AND Room != R128", ""};
		int[] allLabels = {0, 1};
		
		BTree testTree = new BTree();
		testTree.buildTreeFromRules(allRules, allLabels);
		
		testTree.getRulesFromTree();
	}
	
	
	public void testBuildTreeFromRules2()
	{

		String[] allRules = {"ID_key_room_count > 1.0", "room = R128", ""};
		int[] allLabels = {0, 1, 0};
		
		BTree testTree = new BTree();
		testTree.buildTreeFromRules(allRules, allLabels);
		
		testTree.getRulesFromTree();
	}
	
	public void testBuildTreeFromRules3()
	{
		String[] allRules = {"C1", "C2", "C3", ""};
		int[] allLabels = {0, 1, 0, 1};
		
		BTree testTree = new BTree();
		testTree.buildTreeFromRules(allRules, allLabels);
		
		testTree.getRulesFromTree();
	}
	
	public void testBuildTreeFromRules4()
	{
		String[] allRules = {"C1", "C2", "C3", ""};
		int[] allLabels = {0, 1, 1, 0};
		
		BTree testTree = new BTree();
		testTree.buildTreeFromRules(allRules, allLabels);
		
		testTree.getRulesFromTree();
	}
	
}
