package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;

import java.util.ArrayList;

public class Select implements VectorOperator {

	private VectorOperator child;
	private BinaryOp op;
	private int fieldNo;
	private int value;
	private ArrayList<Object>[] field_re;
	private DataType[] type_re;
	private DBColumn[] cols;
	private DBColumn[] result ;

	public Select(VectorOperator child, BinaryOp op, int fieldNo, int value) {
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = value;
	}
	
	@Override
	public void open() {
			child.open();
	}

	@Override
	public DBColumn[] next() {
		cols = child.next();
		if (cols == null){ return null;}
		field_re = new ArrayList[cols.length];
		type_re = new DataType[cols.length];
		result = new DBColumn[cols.length];

		//initial field array
		for (int col_index = 0; col_index < cols.length; col_index++){
			type_re[col_index] = cols[col_index].types[0];
			field_re[col_index] = new ArrayList<Object>();
		}
		int vectorsize = cols[fieldNo].fields.length;

		while (cols != null) {
			Integer[] target_vals = cols[fieldNo].getAsInteger();
			for (int i = 0; i < target_vals.length; i++) {
				if (judge(op, target_vals[i], value)) {
					add_to_result(i);
				}
			}
			if (field_re[fieldNo].size() >= vectorsize){
				emit();
				return result;
			}
			cols = child.next();
		}

		if (field_re[0].size()>0) {
			emit();
			return result;
		}

		return null;

	}

	@Override
	public void close() {
		child.close();
	}

	private void add_to_result(int i){
		for (int col_index = 0; col_index < cols.length; col_index++){
			field_re[col_index].add(cols[col_index].fields[i]);
			type_re[col_index] = cols[col_index].types[0];
		}
	}
	private void emit(){
		for (int j = 0; j < result.length; j++) {
			Object[] field = field_re[j].toArray(new Object[0]);
			result[j] = new DBColumn(field, new DataType[]{type_re[j]});
		}
	}

	private boolean judge(BinaryOp op, int value_choose, int value) {
		switch (op) {
			case LT:
				return (value_choose < value);
			case LE:
				return (value_choose <= value);
			case EQ:
				return (value_choose == value);
			case NE:
				return (value_choose != value);
			case GT:
				return (value_choose > value);
			case GE:
				return (value_choose >= value);
			default:
				return false;
		}
	}
}
