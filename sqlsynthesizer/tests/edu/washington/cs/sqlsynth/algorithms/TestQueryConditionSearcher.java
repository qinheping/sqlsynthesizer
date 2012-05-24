package edu.washington.cs.sqlsynth.algorithms;

import java.util.Collection;
import java.util.LinkedList;

import edu.washington.cs.sqlsynth.entity.SQLSkeleton;
import edu.washington.cs.sqlsynth.entity.TableInstance;
import edu.washington.cs.sqlsynth.util.TableInstanceReader;
import edu.washington.cs.sqlsynth.algorithms.QueryConditionSearcher;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestQueryConditionSearcher extends TestCase {
	
	public static Test suite() {
		return new TestSuite(TestQueryConditionSearcher.class);
	}
	
	public void testFindColumns()
	{
		TableInstance input1 = TableInstanceReader.readTableFromFile("./dat/5_1_3/id_class_5_1_3");
		TableInstance input2 = TableInstanceReader.readTableFromFile("./dat/5_1_3/id_enroll_5_1_3");
		TableInstance output = TableInstanceReader.readTableFromFile("./dat/5_1_3/output_5_1_3");
		
		Collection<TableInstance> inputs = new LinkedList<TableInstance>();
		inputs.add(input1);
		inputs.add(input2);
		SQLSkeletonCreator creator = new SQLSkeletonCreator(inputs, output);
		SQLSkeleton skeleton = creator.inferSQLSkeleton();
		
		System.out.println("input 1:");
		System.out.println(input1);
		System.out.println("input 2:");
		System.out.println(input2);
		
		System.out.println("number of join columns: " + skeleton.getJoinPairNum());
		
		SQLQueryCompletor completor = new SQLQueryCompletor(skeleton);
		completor.addInputTable(input1);
		completor.addInputTable(input2);
		completor.setOutputTable(output);
		
		QueryConditionSearcher testSearcher = new QueryConditionSearcher(completor);
		testSearcher.findColumns("NOT (idkey_room_count > 1.0 AND NOT (room = R128))");
	}
}
