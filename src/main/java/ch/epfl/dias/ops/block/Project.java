package ch.epfl.dias.ops.block;

import ch.epfl.dias.store.column.DBColumn;

import java.util.ArrayList;

public class Project implements BlockOperator {

	private BlockOperator child;
	private int[] columns;

	public Project(BlockOperator child, int[] columns) {
		this.child = child;
		this.columns = columns;
	}

	public DBColumn[] execute() {
		DBColumn[] cols = child.execute();
		DBColumn[] result = new DBColumn[columns.length];
		int index = 0;
		for (int col: columns){
			result[index] = cols[col];
			index++;
		}
		return result;
	}
}
