package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.*;
import java.util.stream.Stream;

public class Join implements VectorOperator {

	private VectorOperator leftChild;
	private VectorOperator rightChild;
	private int leftFieldNo;
	private int rightFieldNo;
	private Hashtable<Object, ArrayList<Integer>> table ;
	private ArrayList<Integer> match;
	private ArrayList<Object>[] field_re;
	private DataType[] type_re;
	private ArrayList<DBColumn[]> col_left;
	private int table_index = 0;
	private int  lf_cols_len;
	private int  currentpos=0;


	public Join(VectorOperator leftChild, VectorOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
		table = new Hashtable<Object, ArrayList<Integer>>();
		col_left = new ArrayList<DBColumn[]>();
	}

	@Override
	public void open() {
		leftChild.open();
		rightChild.open();
		DBColumn[] lf_cols = leftChild.next();
		lf_cols_len = lf_cols.length;
		do {
			col_left.add(lf_cols);
			buildHashTable(lf_cols[leftFieldNo]);
			lf_cols = leftChild.next();
		}while (lf_cols != null);
	}

	@Override
	public DBColumn[] next() {
		DBColumn[] rg_cols = rightChild.next();
		if (rg_cols == null){return null;}
		int vectorsize =  rg_cols[rightFieldNo].fields.length;
		type_re = new DataType[lf_cols_len + rg_cols.length];
		field_re = new ArrayList[lf_cols_len + rg_cols.length];
		DBColumn[] result = new DBColumn[lf_cols_len + rg_cols.length];

		//initial field array
		for (int i = 0; i < result.length; i++ ){
			field_re[i] = new ArrayList<Object>();
		}
		while (rg_cols != null) {
			int index_right = 0;
			for (Object t2 : rg_cols[rightFieldNo].fields) {
				if (probe(t2)) {
					for (int index_left: match){
						int row_sum = 0;  // row number until t-th left child
						int t= 0;     //for t-th left child
						do{
							row_sum += col_left.get(t)[leftFieldNo].fields.length;
							t++;
						}while (row_sum <= index_left);

						t--;
						row_sum -= col_left.get(t)[leftFieldNo].fields.length;

						for (int col_num = 0; col_num < result.length; col_num++){
							if (col_num < lf_cols_len) {
								field_re[col_num].add(col_left.get(t)[col_num].fields[index_left - row_sum]);
								type_re[col_num] = col_left.get(t)[col_num].types[0];
							}else {
								field_re[col_num].add(rg_cols[col_num - lf_cols_len].fields[index_right]);
								type_re[col_num] = rg_cols[col_num - lf_cols_len].types[0];
							}
						}
					}

				}
				index_right++;
			}
			rg_cols = rightChild.next();
		}

		if (field_re[0].size() - currentpos > vectorsize) {
			for (int i = 0; i < result.length; i++) {
				Object[] field = field_re[i].subList(currentpos, currentpos+vectorsize).toArray(new Object[0]);
				result[i] = new DBColumn(field, new DataType[]{type_re[i]});
				currentpos += vectorsize;
			}
			return result;
		}

		if (field_re[0].size() - currentpos > 0) {
			for (int i = 0; i < result.length; i++) {
				Object[] field = field_re[i].subList(currentpos, field_re[0].size()-1).toArray(new Object[0]);
				result[i] = new DBColumn(field, new DataType[]{type_re[i]});
				currentpos = field_re[0].size()-1;
			}
			return result;
		}

		return  null;
	}

	@Override
	public void close() {
		leftChild.close();
		rightChild.close();
	}

	public void buildHashTable(DBColumn t){
		for (Object val : t.fields) {
			try {
				table.get(val).add(table_index);
			} catch (NullPointerException e) {
				table.put(val, new ArrayList<Integer>());
				table.get(val).add(table_index);
			}
			table_index++;
		}
	}
	public boolean probe(Object t) {
		match = table.get(t);
		return (match != null);
	}
}
