package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class Scan implements VolcanoOperator {

	private Store store;
	private int index;

	public Scan(Store store) {
		this.store  = store;
	}

	@Override
	public void open() {
		index = 0;
	}

	@Override
	public DBTuple next() {
		return store.getRow(index++);
	}

	@Override
	public void close() {
		index = 0;
	}
}