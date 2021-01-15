package com.beyondsquare;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("serial")
public class FinAlyzerData implements Serializable {
	
	@JsonProperty("data")
	private List<List<String>> data;
	
	@JsonProperty("columnDef")
	private List<Map<String,String>> columnDef;

	public List<List<String>> getData() {
		return data;
	}

	public void setData(List<List<String>> data) {
		this.data = data;
	}

	public List<Map<String,String>> getColumnDef() {
		return columnDef;
	}

	public void setColumnDef(List<Map<String,String>> columnDef) {
		this.columnDef = columnDef;
	}

}
