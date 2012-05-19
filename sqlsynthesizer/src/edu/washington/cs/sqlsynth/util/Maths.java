package edu.washington.cs.sqlsynth.util;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class Maths {
	
	public static<T> Collection<Collection<T>> allSubset(Collection<T> ts) {
		
		List<T> all = new LinkedList<T>();
		all.addAll(ts);
		int size = ts.size();
		if(size == 0) {
			return Collections.emptySet();
		}
		
		Collection<Collection<T>> ret = new LinkedList<Collection<T>>();
		int N = size;
		int allMasks = (1 << N);
		for (int i = 1; i < allMasks; i++) {
			Collection<T> list = new LinkedList<T>();
			for (int j = 0; j < N; j++)
				if ((i & (1 << j)) > 0) {// The j-th element is used
//					System.out.print((j + 1) + " ");
					list.add(all.get(j));
				}
//			System.out.println();
			ret.add(list);
		}
		
		return ret;
	}
	
}
