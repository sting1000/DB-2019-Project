package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements VolcanoOperator {

	private VolcanoOperator child;
	private int[] fieldNo;

	public Project(VolcanoOperator child, int[] fieldNo) {
		this.child = child;
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		child.open();
	}

	@Override
	public DBTuple next() {
		DBTuple ch = child.next();
		if (ch.eof) {return new DBTuple();}
		int l = fieldNo.length;
		Object[] field = new Object[l];
		DataType[] type = new DataType[l];
		for (int num = 0; num < l; num++){
			field[num] = ch.fields[fieldNo[num]];
			type[num] = ch.types[fieldNo[num]];
		}
		DBTuple tup = new DBTuple(field, type);
		return tup;
	}

	@Override
	public void close() {
		child.close();
	}
}
