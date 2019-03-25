package ch.epfl.dias.store.PAX;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class PAXStore extends Store {

	private DataType[] schema;
	private String filename;
	private String delimiter;
	private int width, height;
	private int tuplesPerPage;
	private DBPAXpage[] pages;

	public PAXStore(DataType[] schema, String filename, String delimiter, int tuplesPerPage) {
		this.schema = schema;
		this.filename = filename;
		this. delimiter = delimiter;
		this.width = schema.length;
		this.tuplesPerPage = tuplesPerPage;
	}

	@Override
	public void load() throws IOException{
		List<String> lines = Files.readAllLines(Paths.get(filename), StandardCharsets.UTF_8);
		this.height = lines.size();
		pages = new DBPAXpage[(int) Math.ceil((double)height/(double)tuplesPerPage)];

		int tupleCount = 0;
		int pageNum = 0;
		String[] fields = new String[tuplesPerPage * width];
		for (String line : lines) {
			tupleCount = tupleCount + 1;
			int itemCount = 0;
			for (String item : line.split(delimiter)) {
				fields[tupleCount-1  + itemCount * tuplesPerPage] =  item;
				itemCount = itemCount +1;
			}
			if (tupleCount % tuplesPerPage == 0){
				pages[pageNum] = new DBPAXpage(fields, schema);
				fields = new String[tuplesPerPage * width];
				tupleCount = 0;
				pageNum = pageNum + 1;
				continue;
			}
			pages[pageNum] = new DBPAXpage(fields, schema);
		}
	}

	@Override
	public DBTuple getRow(int rownumber) {

		DBTuple row;
		if (rownumber < height){
			Object[] temp_field = new Object[width];
			for (int num= 0; num < width; num++) {
				temp_field[num] = pages[rownumber / tuplesPerPage].fields[num * tuplesPerPage + rownumber % tuplesPerPage];
			}
			row = new DBTuple(temp_field, schema);
		}else {
			row = new DBTuple();
		}
		return row;
	}
}
