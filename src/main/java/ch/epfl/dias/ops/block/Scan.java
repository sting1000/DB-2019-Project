package ch.epfl.dias.ops.block;

import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

import java.util.stream.IntStream;

public class Scan implements BlockOperator {

	public ColumnStore store;

	public Scan(ColumnStore store) {
		this.store = store;
	}

	@Override
	public DBColumn[] execute() {
		return store.getColumns(IntStream.range(0, store.width).toArray());
	}
}
