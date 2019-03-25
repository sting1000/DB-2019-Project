package ch.epfl.dias.ops.block;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;
import org.omg.CORBA.OBJ_ADAPTER;

import java.util.*;
import java.util.stream.Stream;

public class Join implements BlockOperator {

	private BlockOperator leftChild;
	private BlockOperator rightChild;
	private int leftFieldNo;
	int rightFieldNo;
	private Hashtable<Object, ArrayList<Integer>> table ;
	private ArrayList<Integer> match;  // to store the target row number list
	private ArrayList<Object>[] field_re;
	private DataType[] type_re;

	public Join(BlockOperator leftChild, BlockOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
		table = new Hashtable<Object, ArrayList<Integer>>();
	}

	public DBColumn[] execute() {
		DBColumn[] lf_cols = leftChild.execute();
		DBColumn[] rg_cols = rightChild.execute();
		type_re = new DataType[lf_cols.length + rg_cols.length];
		field_re = new ArrayList[lf_cols.length + rg_cols.length];
		DBColumn[] result = new DBColumn[lf_cols.length + rg_cols.length];


		buildHashTable(lf_cols[leftFieldNo]);


		//initial field array
		for (int i = 0; i < result.length; i++ ){
			field_re[i] = new ArrayList<Object>();
		}

		int index_right = 0;
		for (Object t2 : rg_cols[rightFieldNo].fields) {
			if (probe(t2)) {
				for (int index_left: match){
					for (int col_num = 0; col_num < result.length; col_num++){
						if (col_num < lf_cols.length) {
							field_re[col_num].add(lf_cols[col_num].fields[index_left]);
							type_re[col_num] = lf_cols[col_num].types[0];
						}else {
							field_re[col_num].add(rg_cols[col_num - lf_cols.length].fields[index_right]);
							type_re[col_num] = rg_cols[col_num - lf_cols.length].types[0];
						}
					}

				}

			}
			index_right++;
		}

		for (int i = 0; i < result.length; i++ ){
			Object[] field = field_re[i].toArray( new Object[0]);
			result[i] = new DBColumn(field, new DataType[]{type_re[i]});
		}
		return result;

	}

	public void buildHashTable(DBColumn t){
		int index = 0;
		for (Object val : t.fields) {
			try {
				table.get(val).add(index);
			} catch (NullPointerException e) {
				table.put(val, new ArrayList<Integer>());
				table.get(val).add(index);
			}
			index++;
		}
	}
	public boolean probe(Object t) {
		match = table.get(t);
		return (match != null);
	}

}
