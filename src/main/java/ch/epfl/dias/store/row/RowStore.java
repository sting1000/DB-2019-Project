package ch.epfl.dias.store.row;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

import java.util.ArrayList;
import java.util.Arrays;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class RowStore extends Store  {
    //Type of the column, this will be determined after parsing the entire document
    private DataType[] schema;
    private String filename;
    private String delimiter;
    private int width, height;
    private List<String> fields=new ArrayList<>();;

	public RowStore(DataType[] schema, String filename, String delimiter) {
		this.schema = schema;
		this.filename = filename;
		this. delimiter = delimiter;
		this.width = schema.length;
	}

	@Override
	public void load() throws IOException{
		try {
			List<String> lines = Files.readAllLines(Paths.get(filename), StandardCharsets.UTF_8);
			fields = new ArrayList<>();
			int height = 0;
			for (String line : lines) {
				height = height + 1;
				fields.addAll(Arrays.asList(line.split(delimiter)));
			}
			this.height = height;
		}catch (IOException e){
			System.err.println(e);
		}
	}

	@Override
	public DBTuple getRow(int rownumber) {
		DBTuple row;
		if (rownumber < height){
			List<String> field = fields.subList(rownumber * width, (rownumber + 1) * width);
			row = new DBTuple(field.toArray(), schema);
		}else {
			row = new DBTuple();
		}
		return row;
	}
}
