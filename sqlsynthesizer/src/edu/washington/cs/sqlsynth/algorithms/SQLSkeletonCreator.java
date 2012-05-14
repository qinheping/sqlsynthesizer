package edu.washington.cs.sqlsynth.algorithms;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import edu.washington.cs.sqlsynth.entity.SQLSkeleton;
import edu.washington.cs.sqlsynth.entity.TableInstance;

public class SQLSkeletonCreator {

	public final List<TableInstance> inputTables;
	public final TableInstance outputTable;
	
	public SQLSkeletonCreator(Collection<TableInstance> inputTables,
			TableInstance outputTable) {
		this.inputTables = new LinkedList<TableInstance>();
		this.inputTables.addAll(inputTables);
		this.outputTable = outputTable;
	}
	
	public SQLSkeleton inferSQLSkeleton() {
		throw new RuntimeException();
	}
}