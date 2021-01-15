package com.beyondsquare.spark.service;

import java.util.List;
import java.util.Map;

public interface FinAlyzerDataService {
	
	public List<List<String>> pivotData(List<List<String>> input_data, List<Map<String,String>> columnDef );
	
	public List<List<String>> unpivotData(List<List<String>> input_data, List<Map<String,String>> columnDef );
	

}
