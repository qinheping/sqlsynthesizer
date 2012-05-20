package edu.washington.cs.sqlsynth.algorithms;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.LinkedList;
import java.util.HashSet;

import java.io.*;

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
		this.callDecisionTree();
		
		System.out.println("---------------------------------------------End of QueryConditionSearcherd---------------------------------------------");
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
		
		List<TableInstance> tables = completor.getSkeleton().computeJoinTableWithoutUnmatches();
		
		for (int i = 0; i < tables.size(); ++i){
			TableInstance table = tables.get(i);
			String relationName = table.getTableName();
			List<TableColumn> columns = table.getColumns();
			
			FastVector attributes = new FastVector(columns.size());
			
			for (int j = 0; j < columns.size(); ++j){
				if (columns.get(j).getType() == TableColumn.ColumnType.String)
				{
					
					FastVector tmpVector = new FastVector();
					for (int k = 0; k< table.getRowNum(); ++k)
					{
						if (!tmpVector.contains(table.getRowValues(k).get(j)))
						{
							tmpVector.addElement(table.getRowValues(k).get(j));
						}
					}
					attributes.addElement(new Attribute(columns.get(j).getFullName(), tmpVector));
				}
				else
				{
					attributes.addElement(new Attribute(columns.get(j).getColumnName()));
				}
				
			}
			
			FastVector tmpVector = new FastVector(2);
			tmpVector.addElement("0");
			tmpVector.addElement("1");
			
			attributes.addElement(new Attribute("class", tmpVector));
			
			
			Instances inputData = new Instances(relationName, attributes, table.getRowNum());
			inputData.setClassIndex(inputData.numAttributes() - 1);
			
			System.out.println(inputData.numAttributes() - 1);
			
			allData.add(i, inputData);
			
		}
	}
	
	private boolean isPositive(List<Object> tuple1, List<Object> tuple2, List<Integer> matchList, List<TableColumn.ColumnType> matchType)
	{
		
		boolean ret = true;
		
		for (int i = 0; i<tuple1.size(); ++i)
		{
			if (matchList.get(i)!=-1)
			{
				if (matchType.get(i) == TableColumn.ColumnType.Integer)
				{	
					
					
					
					if (!((tuple1.get(i)).toString()).equals(((tuple2.get(matchList.get(i))).toString())))
					{
						ret = false;
						break;
					}
				}
				else
				{
					if (!((String)(tuple1.get(i))).equals((String)(tuple2.get(matchList.get(i)))))
					{
						ret = false;
						break;
					}
				
				}
			}
		}
		
		return ret;
	}
	

	
	private void getLabelWeightInfo()
	{
		List<TableInstance> tables = completor.getSkeleton().computeJoinTableWithoutUnmatches();
		
		TableInstance output = completor.getOutputTable();
		
		
		
		for (int i = 0; i<tables.size(); ++i)
		{
			TableInstance table = tables.get(i);
			
			HashSet<Integer> usedIdx = new HashSet();
			
			double posWeight = 0.5;
			double negWeight = 0.5;
			
			
			LinkedList<Integer> matchList = new LinkedList<Integer>();
			LinkedList<TableColumn.ColumnType> matchType = new LinkedList<TableColumn.ColumnType>();
			
			for (int j = 0; j<table.getColumnNum(); ++j)
			{
				int idx = -1;
				TableColumn.ColumnType type = TableColumn.ColumnType.Integer;
				for (int k = 0; k<output.getColumnNum(); ++k)
				{
					if (table.getColumn(j).getColumnName().equals((output.getColumn(k).getColumnName())) )
					{
						idx = k;
						type = output.getColumn(k).getType();
						break;
					}
				}
				matchList.add(idx);
				matchType.add(type);
				
			}
			
			
			for (int j = 0; j<table.getRowNum(); ++j)
			{
				List<Object> tmp_candidate = table.getRowValues(j);
				for (int k = 0; k<output.getRowNum(); ++k)
				{
					List<Object> tmp_output = output.getRowValues(k);
					if (this.isPositive(tmp_candidate, tmp_output, matchList, matchType))
					{
						usedIdx.add(j);
						break;
					}
				}
			}

			negWeight = (usedIdx.size()+0.5)/(table.getRowNum()+1);
			
			posWeight = 1-negWeight;
			
			posWeight = posWeight/usedIdx.size();
			
			negWeight = negWeight/(table.getRowNum() - usedIdx.size());
			
			for (int j = 0; j<table.getRowNum(); ++j)
			{
				Instance inst = new Instance(allData.get(i).numAttributes());
				for (int k = 0; k<table.getColumnNum(); ++k)
				{
					if (table.getColumns().get(k).isIntegerType())
					{
						inst.setValue(allData.get(i).attribute(k), Double.parseDouble( (table.getRowValues(j).get(k)).toString()));
					}
					else
					{
						System.out.println(table.getRowValues(j).get(k));
						inst.setValue(allData.get(i).attribute(k), ((String)(table.getRowValues(j).get(k))));
					}
				}
				if (usedIdx.contains(j))
				{
//					inst.setClassValue("1");
					inst.setValue(allData.get(i).attribute(table.getColumnNum()), "1");
//					inst.setWeight(posWeight);
				}
				else
				{
//					inst.setClassValue("0");
					inst.setValue(allData.get(i).attribute(table.getColumnNum()), "0");
//					inst.setWeight(negWeight);
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

			
			System.out.println("----------------------------------Building tree is done----------------------------------");
			try {
				System.out.println(tree.toSource("1"));
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			System.out.println("----------------------------------   More to do here   ----------------------------------");
		}
		
		
	}
	
	private void getQueryConditions(J48 tree)
	{
		
	}
	

}
