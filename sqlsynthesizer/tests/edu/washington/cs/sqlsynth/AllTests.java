package edu.washington.cs.sqlsynth;

import edu.washington.cs.sqlsynth.algorithms.TestAggregateExprInferrer;
import edu.washington.cs.sqlsynth.algorithms.TestSQLCompletor;
import edu.washington.cs.sqlsynth.algorithms.TestSQLSkeletonCreator;
import edu.washington.cs.sqlsynth.db.TestDatabaseConnection;
import edu.washington.cs.sqlsynth.entity.TestSQLSkeleton;
import edu.washington.cs.sqlsynth.entity.TestTableInstance;
import edu.washington.cs.sqlsynth.util.TestTableInstanceReader;
import edu.washington.cs.sqlsynth.util.TestTableUtils;
import edu.washington.cs.sqlsynth.util.TestUtilAndMath;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Run this tests after any code modification to ensure nothing breaks
 * */
public class AllTests extends TestCase {

	public static Test suite() {
		TestSuite suite = new TestSuite();
		
		suite.addTest(TestAggregateExprInferrer.suite());
		suite.addTest(TestSQLCompletor.suite());
		suite.addTest(TestSQLSkeletonCreator.suite());
		suite.addTest(TestDatabaseConnection.suite());
		suite.addTest(TestSQLSkeleton.suite());
		suite.addTest(TestTableInstance.suite());
		suite.addTest(TestTableInstanceReader.suite());
		suite.addTest(TestUtilAndMath.suite());
		suite.addTest(TestTableUtils.suite());
		
		return suite;
	}
	
}
