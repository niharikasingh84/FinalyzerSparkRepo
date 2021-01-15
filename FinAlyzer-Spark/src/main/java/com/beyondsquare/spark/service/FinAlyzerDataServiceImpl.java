package com.beyondsquare.spark.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;

@Service("finalyzer_data_service")
public class FinAlyzerDataServiceImpl implements FinAlyzerDataService  {
	
	@Override
	public List<List<String>> pivotData(List<List<String>> input_data, List<Map<String,String>> columnDef) {
		SparkSession spark = start();
		List<String> column = input_data.get(0);
		List<String> mergeInputData = new ArrayList<>();
		String delimeter = ",";

		for (int i = 1; i < input_data.size(); i++) {
			List<String> list = input_data.get(i);
			mergeInputData.add(String.join(delimeter, list));
		}
		Dataset<String> dataDs = spark.createDataset(mergeInputData, Encoders.STRING());
		
		List<String> list_as_table_records = new ArrayList<>();
		for(int i=0;i<column.size();i++) {
			list_as_table_records.add("split(value, ',')["+i+"] as " + column.get(i));
		}
		Dataset<Row> dataset = dataDs.selectExpr(list_as_table_records.toArray(new String[0]));
		Date date = new Date();
		System.out.println("Time 2- Loaded Data Frame:"+date);
		
	    Map<String,String> column_dimension = new HashMap<>();
		for(Map<String,String> entry: columnDef) {
			if(entry.get("pivot_dimension").equalsIgnoreCase("R")) {
				column_dimension.put("Row_Dimension",entry.get("field_name") );
			}
			if(entry.get("pivot_dimension").equalsIgnoreCase("C")) {
				column_dimension.put("Column_Dimension",entry.get("field_name"));
			}
			if(entry.get("pivot_dimension").equalsIgnoreCase("D")) {
				column_dimension.put("Data_Dimension",entry.get("field_name"));
			}	
		}
	
		Dataset<Row> ds = dataset.withColumn(column_dimension.get("Data_Dimension"), dataset.col(column_dimension.get("Data_Dimension")).cast("decimal(20,0)")); 
		Date date2 = new Date();
		System.out.println("Time 3-Cast data Set:"+date2);
		ds.show();
        Date date7 = new Date();
		System.out.println("Time -DS dataset printed:"+date7);
		Dataset<Row> pivot_data = ds.groupBy(column_dimension.get("Row_Dimension")).pivot(column_dimension.get("Column_Dimension")).sum(column_dimension.get("Data_Dimension"));
		
		Date date3 = new Date();
		System.out.println("Time 4-Pivot Operation Done:"+date3); 
		pivot_data.show();
		Date date6 = new Date();
		System.out.println("Time 6-Show Pivot data:"+date6); 
		/*
		 * List<List<String>> response_list = new ArrayList<>();
		 * response_list.add(Arrays.asList(pivot_data.columns())); List<Row> row_list =
		 * pivot_data.collectAsList(); Date date5 = new Date();
		 * System.out.println("Time 5- Row Collect:"+date5);
		 * System.out.println("Row List size "+row_list.size()); for(Row row : row_list)
		 * { List<String> row_entry = new ArrayList<>(); for(int i=0;i<row.length();i++)
		 * { row_entry.add(String.valueOf(row.get(i))); } response_list.add(row_entry);
		 * } Date date4 = new Date();
		 * System.out.println("Time 6-Loaded data frame to Java object:"+date4);
		 * System.out.println("REsponse List size "+response_list.size()); return
		 * response_list;
		 */
		return null;
	}
	
	@Override
	public List<List<String>> unpivotData(List<List<String>> input_data, List<Map<String, String>> columnDef) {
		SparkSession spark = start();
		List<String> column = input_data.get(0);
		List<String> mergeInputData = new ArrayList<>();
		String delimeter = ",";

		for (int i = 1; i < input_data.size(); i++) {
			List<String> list = input_data.get(i);
			mergeInputData.add(String.join(delimeter, list));
		}
		Dataset<String> dataDs = spark.createDataset(mergeInputData, Encoders.STRING());
		List<String> list_as_table_records = new ArrayList<>();
		for(int i=0;i<column.size();i++) {
			list_as_table_records.add("split(value, ',')["+i+"] as " + column.get(i));
		}
		Dataset<Row> dataset = dataDs.selectExpr(list_as_table_records.toArray(new String[0]));
        dataset.createOrReplaceTempView("pivotTable");
	    Dataset<Row> sql_data = spark.sql("SELECT Location,Product,Scenario,Business_Line,stack(2,'Revenue',Revenue,'Cost',Cost) as (Grouping,Amount) from pivotTable");
	    sql_data.show();
	    
		  List<List<String>> response_list = new ArrayList<>();
			response_list.add(Arrays.asList(sql_data.columns()));

			List<Row> sql_data_list = sql_data.collectAsList();
			for (Row row : sql_data_list) {
			   List<String> row_list = new ArrayList<String>();
			   for(int i=0;i<row.length();i++)
			      row_list.add(String.valueOf(row.get(i)));
			   response_list.add(row_list);
			} 
	         return response_list;
		
	}
	
	private SparkSession start() {
		SparkSession spark = SparkSession.builder().appName("Unpivot Data").master("local").getOrCreate();
		return spark;
	}
	

}
