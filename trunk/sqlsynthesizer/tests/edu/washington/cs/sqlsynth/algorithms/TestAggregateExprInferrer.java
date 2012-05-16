package edu.washington.cs.sqlsynth.algorithms;

import java.util.List;
import java.util.Map;

import edu.washington.cs.sqlsynth.entity.AggregateExpr;
import edu.washington.cs.sqlsynth.entity.SQLSkeleton;
import edu.washington.cs.sqlsynth.entity.TableColumn;
import edu.washington.cs.sqlsynth.entity.TableInstance;
import edu.washington.cs.sqlsynth.util.TableInstanceReader;
import junit.framework.TestCase;

public class TestAggregateExprInferrer extends TestCase {

	public void test1() {
		TableInstance input1 = TableInstanceReader.readTableFromFile("./dat/id_name");
		TableInstance input2 = TableInstanceReader.readTableFromFile("./dat/id_salary");
		TableInstance output = TableInstanceReader.readTableFromFile("./dat/id_name_salary");
		SQLSkeleton skeleton = TestSQLSkeletonCreator.createSampleSkeleton();
		SQLQueryCompletor completor = new SQLQueryCompletor(skeleton);
		completor.addInputTable(input1);
		completor.addInputTable(input2);
		completor.setOutputTable(output);
		
		//create the inferrer
		AggregateExprInfer aggInfer = new AggregateExprInfer(completor);
		Map<Integer, AggregateExpr> aggrExprs = aggInfer.inferAggregationExprs();
		List<TableColumn> groupbyColumns = aggInfer.inferGroupbyColumns();
		
		System.out.println(aggrExprs);
		System.out.println(groupbyColumns);
		System.out.println(skeleton.getAllJoinConditions());
		System.out.println(skeleton.getProjectColumns());
		
		assertTrue(aggrExprs.isEmpty());
		assertTrue(groupbyColumns.isEmpty());
	}
	
}
