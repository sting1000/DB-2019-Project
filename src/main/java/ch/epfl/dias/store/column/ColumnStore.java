package ch.epfl.dias.store.column;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class ColumnStore extends Store {

	private DataType[] schema;
	private String filename;
	private String delimiter;
	public int width, height;
	private String[] fields;

	public ColumnStore(DataType[] schema, String filename, String delimiter) {
		this.schema = schema;
		this.filename = filename;
		this. delimiter = delimiter;
		this.width = schema.length;
	}

	@Override
	public void load() throws IOException{
		List<String> lines = Files.readAllLines(Paths.get(filename), StandardCharsets.UTF_8);
		this.height = lines.size();
		fields = new String[height * width];
		int tupleCount = 0;
		for (String line : lines) {
			int itemCount = 0;
			for (String item : line.split(delimiter)) {
				fields[tupleCount  + itemCount * height] =  item;
				itemCount = itemCount +1;
			}
			tupleCount = tupleCount + 1;
		}

	}

	public DBColumn getColumn(int colnumber) {
		String[] result =  new String[height];
		for (int i = 0; i < height; i = i + 1){
			result[i] =  fields[colnumber * height + i];
		}
		DBColumn col = new DBColumn(result, new DataType[]{schema[colnumber]});
		return col;
	}

	@Override
	public DBColumn[] getColumns(int[] columnsToGet) {
		if (columnsToGet.length == 0){
			columnsToGet = IntStream.range(0, width).toArray();
		}
		DBColumn[] cols = new DBColumn[columnsToGet.length];
		int i = 0;
		for (int colNum: columnsToGet ){
			cols[i] = getColumn(colNum);
			i = i + 1;
		}
		return cols;
	}

}
