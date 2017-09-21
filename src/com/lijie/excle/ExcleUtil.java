package com.lijie.excle;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;

public class ExcleUtil {
	
	private StringBuilder sb = null;
	
	private long bytesRead = 0;
	
	public String parseExcleData(InputStream in) {
		
		HSSFWorkbook hwk = null;
		try {
			hwk = new HSSFWorkbook(in);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		HSSFSheet sheet = hwk.getSheetAt(0);
		
		Iterator<Row> iterator = sheet.iterator();
		
		sb = new StringBuilder();
		
		while (iterator.hasNext()) {
			Row row = iterator.next();
			
			Iterator<Cell> cellIterator = row.cellIterator();
			
			while (cellIterator.hasNext()) {
				Cell next = cellIterator.next();
				
				switch (next.getCellType()) {
					case Cell.CELL_TYPE_BOOLEAN:
						bytesRead++;
						sb.append(next.getBooleanCellValue() + "\t");
						break;
					case Cell.CELL_TYPE_NUMERIC:
						bytesRead++;
						sb.append(next.getNumericCellValue() + "\t");
						break;
					case Cell.CELL_TYPE_STRING:
						bytesRead++;
						sb.append(next.getStringCellValue() + "\t");
						break;
					default:
						break;
				}
			}
			sb.append("\n");
		}
		return sb.toString();
	}
	
	public long getByteRead() {
		return bytesRead;
	}
}
