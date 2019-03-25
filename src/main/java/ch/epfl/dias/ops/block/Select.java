package ch.epfl.dias.ops.block;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class Select implements BlockOperator {

	private BlockOperator child;
	private BinaryOp op;
	private int fieldNo;
	private int value;

	public Select(BlockOperator child, BinaryOp op, int fieldNo, int value) {
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = value;
	}

	@Override
	public DBColumn[] execute() {
		DBColumn[] cols = child.execute();

		Integer[] target_vals = cols[fieldNo].getAsInteger();
		ArrayList<Integer> target_index = new ArrayList<Integer>();
		for (int i = 0; i < target_vals.length; i++){
			if(judge(op, target_vals[i], value)) {
				target_index.add(i);
			}
		}


		int index = 0;
		Object[] fields;
		DBColumn[] result = new DBColumn[cols.length];
		for (DBColumn col: cols){
			fields = new Object[target_index.size()];
			for (int j = 0; j < target_index.size(); j++){
					fields[j] = col.fields[target_index.get(j)];
			}
			result[index] = new DBColumn(fields, cols[index].types);
			index++;
		}
		return  result;
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
