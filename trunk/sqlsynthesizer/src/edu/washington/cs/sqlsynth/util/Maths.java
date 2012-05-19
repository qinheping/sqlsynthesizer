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
	
	public static<T> List<List<T>> allCombination(List<List<T>> ts) {
		Utils.checkTrue(ts.size() > 0);
		for(Collection<T> t : ts) {
			Utils.checkTrue(!t.isEmpty(), "the given collection cannot be empty");
		}
		//Collection<List<T>> ret = new LinkedList<List<T>>();
		
		List<T> firstList = ts.get(0);
		List<List<T>> init = new LinkedList<List<T>>();
		for(T t : firstList) {
			List<T> l = new LinkedList<T>();
			l.add(t);
			init.add(l);
		}
		//do pair wise
		List<List<T>> result = init;
		for(int i = 1; i < ts.size(); i++) {
			result = pairWise(ts.get(i), result);
		}
		
		
		return result;
	}
	
	public static<T> List<List<T>> allCombination(List<T>...ts) {
		Utils.checkTrue(ts.length > 0);
		for(Collection<T> t : ts) {
			Utils.checkTrue(!t.isEmpty(), "the given collection cannot be empty");
		}
		//Collection<List<T>> ret = new LinkedList<List<T>>();
		
		List<T> firstList = ts[0];
		List<List<T>> init = new LinkedList<List<T>>();
		for(T t : firstList) {
			List<T> l = new LinkedList<T>();
			l.add(t);
			init.add(l);
		}
		//do pair wise
		List<List<T>> result = init;
		for(int i = 1; i < ts.length; i++) {
			result = pairWise(ts[i], result);
		}
		
		
		return result;
	}
	
	public static<T> List<List<T>> pairWise(List<T> l1, List<List<T>> l2) {
		List<List<T>> ret = new LinkedList<List<T>>();
		
		for(T t : l1) {
			for(List<T> lt : l2) {
				List<T> newList = new LinkedList<T>();
				newList.add(t);
				newList.addAll(lt);
				ret.add(newList);
			}
		}
		
		Utils.checkTrue(ret.size() == l1.size() * l2.size());
		
		return ret;
	}
	
}
