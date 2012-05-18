package edu.washington.cs.sqlsynth.algorithms;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.LinkedList;
import java.util.HashSet;

import edu.washington.cs.sqlsynth.entity.QueryCondition;
import edu.washington.cs.sqlsynth.entity.TableInstance;
import edu.washington.cs.sqlsynth.entity.TableColumn;

// firstly, use simpely weka
import weka.core.Instances;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;

import weka.classifiers.trees.*;



public class QueryConditionSearcher {

	public final SQLQueryCompletor completor;
	
	private List<Instances> allData;
//	private List<Double> weight;
	
	public QueryConditionSearcher(SQLQueryCompletor completor) {
		this.completor = completor;
		allData = new LinkedList<Instances>();
//		weight = new LinkedList<Double>();
		
		
		this.getConstructionInfo();
		this.getLabelWeightInfo();
	}
	
	public Collection<QueryCondition> inferQueryConditions() {
		
		
		
		//use decision tree to infer query condition
		//throw new RuntimeException("");
		return Collections.emptySet();
	}
	
	private void getConstructionInfo()
	{
//		allData = new LinkedList<Instances>();
		allData.clear();
		
		List<TableInstance> tables = completor.getSkeleton().getTables();
		
		for (int i = 0; i < tables.size(); ++i){
			TableInstance table = tables.get(i);
			String relationName = table.getTableName();
			List<TableColumn> columns = table.getColumns();
			
			FastVector attributes = new FastVector(columns.size());
			
			for (int j = 0; j < columns.size(); ++j){
				if (columns.get(j).getType() == TableColumn.ColumnType.String)
				{
					attributes.addElement(new Attribute(columns.get(j).getFullName(), (FastVector) null));
				}
				else
				{
					attributes.addElement(new Attribute(columns.get(j).getColumnName()));
				}
				
			}
			
//			attributes.addElement(new Attribute("class"));
			
			
			
			Instances inputData = new Instances(relationName, attributes, table.getRowNum());
			allData.add(i, inputData);
			
		}
	}
	
	private void getLabelWeightInfo()
	{
		List<TableInstance> tables = completor.getSkeleton().getTables();
		
		TableInstance output = completor.getOutputTable();
		
		
		
		for (int i = 0; i<tables.size(); ++i)
		{
			TableInstance table = tables.get(i);
			
			HashSet<Integer> usedIdx = new HashSet();
			
			double posWeight = 0.5;
			double negWeight = 0.5;
			
			for (int j = 0; j<table.getRowNum(); ++j)
			{
				usedIdx.add(j);
			}
			
			for (int j = 0; j<table.getColumnNum(); ++j)
			{
				boolean flag = false;
				for (int k = 0; k<output.getColumnNum(); ++k)
				{
					if (table.hasColumn(output.getColumns().get(k).getFullName()))
					{
						flag = true;
						break;
					}
				}
				
				if (flag)
				{
					for (int k = 0; k<table.getRowNum(); ++k)
					{
						boolean eqFlag = false;
						for (int l = 0; l<output.getRowNum(); ++l)
						{
							if (table.getRowValues(k) == output.getRowValues(l))
							{
								eqFlag = true;
								break;
							}
						}
						
						if (!eqFlag)
						{
							usedIdx.remove(k);
						}
					}
				}
			}
			
			posWeight = usedIdx.size()/table.getRowNum();
			negWeight = 1-posWeight;
			
//			weight.add(posWeight);
			
			for (int j = 0; j<table.getRowNum(); ++j)
			{
				Instance inst = new Instance(allData.get(i).numAttributes());
				for (int k = 0; k<table.getColumnNum()-1; ++k)
				{
					if (table.getColumns().get(k).isIntegerType())
					{
						inst.setValue(allData.get(i).attribute(k), (Double)(table.getRowValues(i).get(j)));
					}
					else
					{
						inst.setValue(allData.get(i).attribute(k), (String)(table.getRowValues(i).get(j)));
					}
				}
				if (usedIdx.contains(j))
				{
					inst.setClassValue(1);
					inst.setWeight(posWeight);
				}
				else
				{
					inst.setClassValue(0);
					inst.setWeight(negWeight);
				}
				
				
				allData.get(i).add(inst);
			}
			
		}
	}
	
	private void callDecisionTree()
	{
		String[] options = new String[1];
		options[0] = "-U";
		
		for (int i = 0; i<allData.size(); ++i)
		{
			J48 tree = new J48();
			try {
				tree.setOptions(options);
				tree.buildClassifier(allData.get(i));
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		
	}
	
	private void getQueryConditions(J48 tree)
	{
		
	}
	

}
