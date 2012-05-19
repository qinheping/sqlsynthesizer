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
	
	public void testCombination() {
		List<String> l1 = new LinkedList<String>();
		l1.add("a");
		l1.add("b");
		l1.add("c");
		List<String> l2 = new LinkedList<String>();
		l2.add("1");
		l2.add("2");
		l2.add("3");
		List<String> l3 = new LinkedList<String>();
		l3.add("x");
		l3.add("y");
		List<List<String>> all = Maths.allCombination(l1, l2, l3);
		assertEquals(all.size(), 18);
		System.out.println(all);
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
