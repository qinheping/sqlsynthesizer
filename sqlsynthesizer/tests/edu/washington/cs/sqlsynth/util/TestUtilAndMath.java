package edu.washington.cs.sqlsynth.util;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestUtilAndMath extends TestCase {
	
	public static Test suite() {
		return new TestSuite(TestUtilAndMath.class);
	}
	
	public void testAllComb() {
		this.allCombi(3);
	}
	
	public void testEnum() {
		List<String> strList = new LinkedList<String>();
		strList.add("a");
		strList.add("b");
		strList.add("c");
		Collection<Collection<String>> ret = Maths.allSubset(strList);
		for(Collection<String> c: ret) {
			System.out.println(c);
		}
		assertEquals(7, ret.size());
	}

	public void allCombi(int N) {
		int allMasks = (1 << N);
		for (int i = 1; i < allMasks; i++) {
			for (int j = 0; j < N; j++)
				if ((i & (1 << j)) > 0) // The j-th element is used
					System.out.print((j + 1) + " ");

			System.out.println();
		}
	}

}
