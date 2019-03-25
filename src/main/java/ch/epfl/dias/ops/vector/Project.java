package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements VectorOperator {

	private VectorOperator child;
	private int[] fieldNo;


	public Project(VectorOperator child, int[] fieldNo) {
		this.child = child;
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		child.open();
	}

	@Override
	public DBColumn[] next() {
		DBColumn[] cols = child.next();
		if (cols == null){return null;}
		DBColumn[] result = new DBColumn[fieldNo.length];
		int index = 0;
		for (int col: fieldNo){
			result[index] = cols[col];
			index++;
		}
		return result;
	}

	@Override
	public void close() {
		child.close();
	}
}
