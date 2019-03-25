package ch.epfl.dias.ops.block;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class ProjectAggregate implements BlockOperator {

	private BlockOperator child;
	private Aggregate agg;
	private DataType dt;
	private int fieldNo;
	
	public ProjectAggregate(BlockOperator child, Aggregate agg, DataType dt, int fieldNo) {
		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;
	}

	@Override
	public DBColumn[] execute() {
		DBColumn[] cols = child.execute();
		Object[] field = new Object[1];
		field[0] = agg_impl(cols[fieldNo], agg);
		return new DBColumn[]{new DBColumn(field, new DataType[]{dt})};
	}

	private double agg_impl(DBColumn col, Aggregate agg) {
		if (col.eof) {
			return Double.parseDouble(null);
		} else {
			switch (agg) {
				case COUNT:
					return (double)col.fields.length;

				case SUM:
					double sum = 0;
					for(double field: col.getAsDouble()){
						sum = sum + field;
					}
					return sum;

				case MIN:
					double min = col.getAsDouble()[0];
					for(double field: col.getAsDouble()){
						if ( field < min) {
							min =  field;
						}
					}
					return min;

				case MAX:
					double max = col.getAsDouble()[0];
					for(double field: col.getAsDouble()){
						if ( field > max) {
							max =  field;
						}
					}
					return max;

				case AVG:
					return agg_impl(col, Aggregate.SUM) / agg_impl(col, Aggregate.COUNT);
			}
			return Double.parseDouble(null);
		}
	}
}
