package edu.washington.cs.sqlsynth.entity;


import java.util.LinkedList;

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
	
	public void testBuildTreeFromRules5()
	{
		LinkedList<String> allRules = new LinkedList<String>();
		LinkedList<Integer> allLabels = new LinkedList<Integer>();
		
		allRules.add("C1");
		allRules.add("C2");
		allRules.add("C3");
		allRules.add("");
		
		allLabels.add(0);
		allLabels.add(1);
		allLabels.add(1);
		allLabels.add(0);
		
		BTree testTree = new BTree();
		testTree.buildTreeFromRules(allRules, allLabels);
		testTree.getRulesFromTree();
		
	}
}
