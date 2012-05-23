package edu.washington.cs.sqlsynth.algorithms;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.LinkedList;
import java.util.HashSet;
import java.util.Vector;

import java.io.*;

import edu.washington.cs.sqlsynth.entity.QueryCondition;
import edu.washington.cs.sqlsynth.entity.TableInstance;
import edu.washington.cs.sqlsynth.entity.TableColumn;
import edu.washington.cs.sqlsynth.util.Globals;

// firstly, use simpely weka
import weka.core.Instances;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;

import weka.classifiers.trees.*;
import weka.classifiers.rules.*;
import weka.classifiers.rules.part.ClassifierDecList;
import weka.classifiers.rules.part.MakeDecList;


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
//		this.callDecisionTree();
		this.callRulePART();
		
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
					attributes.addElement(new Attribute(columns.get(j).getColumnName(), tmpVector));
				}
				else
				{
					attributes.addElement(new Attribute(columns.get(j).getColumnName()));
				}
				
			}
			
			
			for (int j = 0; j< columns.size(); ++j)
			{
				if (columns.get(j).getType() == TableColumn.ColumnType.String)
				{
					for (int k = 0; k< columns.size(); ++k)
					{
						if (columns.get(k).getType() == TableColumn.ColumnType.String)
						{
							attributes.addElement(new Attribute(columns.get(j).getColumnName()+"_"+columns.get(k).getColumnName()+"_count"));
						}
						else
						{
							attributes.addElement(new Attribute(columns.get(j).getColumnName()+"_"+columns.get(k).getColumnName()+"_max"));
							attributes.addElement(new Attribute(columns.get(j).getColumnName()+"_"+columns.get(k).getColumnName()+"_min"));
							attributes.addElement(new Attribute(columns.get(j).getColumnName()+"_"+columns.get(k).getColumnName()+"_sum"));
							attributes.addElement(new Attribute(columns.get(j).getColumnName()+"_"+columns.get(k).getColumnName()+"_avg"));
						}
					}
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
				
				int attCount = table.getColumnNum();
				
				
				for (int k = 0; k< table.getColumnNum(); ++k)
				{
					if (table.getColumn(k).getType() == TableColumn.ColumnType.String)
					{
						for (int l = 0; l< table.getColumnNum(); ++l)
						{
							if (table.getColumn(l).getType() == TableColumn.ColumnType.String)
							{
								List<TableInstance> inputTables = completor.getInputTables();
								boolean flag = false;
								for (int m = 0; m<inputTables.size(); ++m)
								{
									TableInstance tmpTable = inputTables.get(m);
									if (tmpTable.hasColumn(table.getColumn(l).getColumnName()) && tmpTable.hasColumn(table.getColumn(k).getColumnName()))
									{
										int rowNum = -1;
										TableColumn col = tmpTable.getColumnByName(table.getColumn(k).getColumnName());
										
										for (int n = 0; n<tmpTable.getRowNum(); ++n)
										{
											if (col.getValue(n).toString().equals(table.getColumn(k).getValue(j).toString()))
											{
												rowNum = n;
												break;
											}
										}
										inst.setValue(allData.get(i).attribute(attCount++), tmpTable.getUniqueCountOfSameKey(table.getColumn(l).getColumnName(), table.getColumn(k).getColumnName(), rowNum));
//										inst.setValue(allData.get(i).attribute(attCount++), tmpTable.getCountOfSameKey(table.getColumn(l).getColumnName(), table.getColumn(k).getColumnName(), rowNum));
										flag = true;
										break;
									}
								}
								if (!flag)
								{
									inst.setValue(allData.get(i).attribute(attCount++), 0);
								}

							}
							else
							{
								
								List<TableInstance> inputTables = completor.getInputTables();
								boolean flag = false;
								
								for (int m = 0; m<inputTables.size(); ++m)
								{
									TableInstance tmpTable = inputTables.get(m);
									if (tmpTable.hasColumn(table.getColumn(l).getColumnName()) && tmpTable.hasColumn(table.getColumn(k).getColumnName()))
									{
										int rowNum = 0;
										TableColumn col = tmpTable.getColumnByName(table.getColumn(k).getColumnName());
										
										for (int n = 0; n<tmpTable.getRowNum(); ++n)
										{
											if (col.getValue(n).toString().equals(table.getColumn(k).getValue(j).toString()))
											{
												rowNum = n;
												break;
											}
										}
										
										inst.setValue(allData.get(i).attribute(attCount++), tmpTable.getMaxOfSameKey(table.getColumn(l).getColumnName(), table.getColumn(k).getColumnName(), rowNum));
										inst.setValue(allData.get(i).attribute(attCount++), tmpTable.getMinOfSameKey(table.getColumn(l).getColumnName(), table.getColumn(k).getColumnName(), rowNum));
										inst.setValue(allData.get(i).attribute(attCount++), tmpTable.getSumOfSameKey(table.getColumn(l).getColumnName(), table.getColumn(k).getColumnName(), rowNum));
										inst.setValue(allData.get(i).attribute(attCount++), tmpTable.getAvgOfSameKey(table.getColumn(l).getColumnName(), table.getColumn(k).getColumnName(), rowNum));
	
										flag = true;
										break;
									}
								}
								if (!flag)
								{
									inst.setValue(allData.get(i).attribute(attCount++), 0);
									inst.setValue(allData.get(i).attribute(attCount++), 0);
									inst.setValue(allData.get(i).attribute(attCount++), 0);
									inst.setValue(allData.get(i).attribute(attCount++), 0);
								}								
							}
						}
					}
				}
				
				if (usedIdx.contains(j))
				{
					int classIdx = allData.get(i).numAttributes() - 1;
					inst.setValue(allData.get(i).attribute(classIdx), "1");
//					inst.setWeight(posWeight);
				}
				else
				{
					int classIdx = allData.get(i).numAttributes() - 1;
					inst.setValue(allData.get(i).attribute(classIdx), "0");
//					inst.setWeight(negWeight);
				}				
				allData.get(i).add(inst);
			}
			
		}
	}
	
	
	
	private void callDecisionTree()
	{
		String[] options = new String[3];
		options[0] = "-U";
		options[1] = "-B";
		options[2] = "-L";
		
		
		
		for (int i = 0; i<allData.size(); ++i)
		{
			J48 tree = new J48();
			try {
//				allData.get(i).deleteAttributeAt(0);
//				allData.get(i).deleteAttributeAt(0);
//				allData.get(i).deleteAttributeAt(0);
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
	
	private void callRulePART()
	{
		String[] options = new String[3];
		options[0] = "-U";
		options[1] = "-B";
		options[2] = "-L";
		
		
		for (int i = 0; i<allData.size(); ++i)
		{
			PART tree = new PART();
			try{
				tree.setOptions(options);
				tree.buildClassifier(allData.get(i));
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
			
			System.out.println("----------------------------------Building tree is done----------------------------------");
			
			System.out.println(tree.toString());
			
			String rules = tree.toString();
/*			
			System.out.println(tree.toSummaryString());
			
			MakeDecList root = tree.getRoot();
			Vector allRules = root.getRules();
			
			System.out.println("See all rules");
			for(Object o : allRules) {
				ClassifierDecList dl = (ClassifierDecList)o;
				System.out.println("see rule: ");
				System.out.println(dl);
				if(!dl.isLeaf()) {
				    System.out.println("No label no break: " + dl.toStringNoLabelNoBreak());
				}
			}
	*/		
			parseRules(rules);
			
			System.out.println("----------------------------------   More to do here   ----------------------------------");
		}
	}
	
	
	private void parseRules(String rules)
	{
		String[] lines = rules.split(System.getProperty("line.separator"));
		
		int startIdx = lines[lines.length - 1].lastIndexOf(":") + 3;
	
		int numRules = Integer.parseInt(lines[lines.length - 1].substring(startIdx));
		
		int ruleIdx = 0;
		
		
		StringBuffer condBuffer = new StringBuffer();
		
		for (int i = 2; i<lines.length-1; ++i)
		{
			
			if (lines[i].length() == 0)
			{
				continue;
			}
			
			if ( lines[i].contains(":") )
			{
				int idx1 = lines[i].lastIndexOf(":")+2;
				int idx2 = lines[i].lastIndexOf("(")-1;
				
				int label = Integer.parseInt(lines[i].substring(idx1, idx2));

				String condition = "";
				if (idx1-3>0)
				{
					condition = lines[i].substring(0, idx1-3);
				}
				
				if (condBuffer.length() != 0)
				{
					condBuffer.append(" "+condition);
				}
				else
				{
					condBuffer.append(condition);
				}
				
				System.out.println(condBuffer);
				condBuffer.delete(0, condBuffer.length());
			}
			else
			{
				if (condBuffer.length() !=0)
				{
					condBuffer.append(" "+lines[i]);
				}
				else
				{
					condBuffer.append(lines[i]);
				}
			}
			
		}
		System.out.println("----------------------------------   End of parse rules   ----------------------------------");
	}
}
