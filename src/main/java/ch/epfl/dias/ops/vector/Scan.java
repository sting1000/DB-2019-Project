package ch.epfl.dias.ops.vector;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;
import org.relaxng.datatype.Datatype;

import javax.xml.crypto.Data;
import javax.xml.datatype.DatatypeConfigurationException;
import java.util.ArrayList;
import java.util.stream.IntStream;

public class Scan implements VectorOperator {

	private Store store;
	public int vectorsize;
	private int index;
	private  DBColumn[] cols;

	public Scan(Store store, int vectorsize) {
		this.store = store;
		this.vectorsize = vectorsize;
		cols = store.getColumns(new int[]{});
	}
	
	@Override
	public void open() {
		index = 0;
	}

	@Override
	public DBColumn[] next() {
		//early materialization
		ArrayList<Object>[] field_re = new ArrayList[cols.length];
		DBColumn[] result = new DBColumn[cols.length];
		DataType[] type_re = new DataType[cols.length];

		if (index >= cols[0].fields.length){
			return null;
		}

		//initial field array
		for (int i = 0; i < result.length; i++ ){
			field_re[i] = new ArrayList<Object>();
		}

		//calculate the row number of result
		int size;
		if (index + vectorsize <= cols[0].fields.length) {
			size = vectorsize;
		}else{
			size = cols[0].fields.length % vectorsize;
		}

		for (int col_index = 0; col_index < cols.length; col_index++){
			for (int row_index = 0; row_index < size; row_index++) {
				field_re[col_index].add(cols[col_index].fields[index + row_index]);
			}
			type_re[col_index] = cols[col_index].types[0];
		}

		index += size;

		for (int i = 0; i < result.length; i++ ){
			Object[] field = field_re[i].toArray( new Object[0]);
			result[i] = new DBColumn(field, new DataType[]{type_re[i]});
		}
		return result;

		//late materialization
//		DBColumn[] whole_id = store.getColumns(new int[]{});
//		DBColumn[] whole = store.getColumns(new int[]{});
//		int[]  id =new int[whole[0].fields.length];
//		for (int i = 0; i<whole.length; i++) {
//			DBColumn idColumn = new DBColumn(whole[i].fields, whole[i].types, id);
//			whole_id[i] = idColumn;
//		}
//		return whole_id

	}

	@Override
	public void close() {
		index = 0;
	}
}
