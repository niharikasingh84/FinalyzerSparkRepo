package com.beyondsquare.spark.controller;

import java.util.Date;
import java.util.List;

import javax.annotation.Resource;

import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.beyondsquare.FinAlyzerData;
import com.beyondsquare.spark.service.FinAlyzerDataService;

@RestController
@RequestMapping("/spark/data")
public class FinAlyzerSparkController {
	
	@Resource(name="finalyzer_data_service")
	FinAlyzerDataService finalyzerDataService;
	
	@RequestMapping(value = "/pivot", method = RequestMethod.POST , consumes = "application/json")
	public List<List<String>> pivot(@RequestBody FinAlyzerData finAlyzerData) {
		Date date = new Date();
		System.out.println("Time 1-Request hits API:"+date); 
		return finalyzerDataService.pivotData(finAlyzerData.getData(), finAlyzerData.getColumnDef());
	}
	
	@RequestMapping(value = "/readFromCSV", method = RequestMethod.POST)
	public String readFromCSV(@RequestParam("file") MultipartFile file) {
		
		return file.getOriginalFilename();
		
	}

}
