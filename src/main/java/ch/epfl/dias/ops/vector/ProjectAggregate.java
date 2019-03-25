package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class ProjectAggregate implements VectorOperator {

	private VectorOperator child;
	private Aggregate agg;
	private DataType dt;
	private int fieldNo;

	public ProjectAggregate(VectorOperator child, Aggregate agg, DataType dt, int fieldNo) {
		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		child.open();
	}

	@Override
	public DBColumn[] next() {
		DBColumn[] cols = child.next();
		Object[] field = new Object[1];
		if (cols == null){
			return null;
		}
		field[0] = agg_impl(cols, agg);
		return new DBColumn[]{new DBColumn(field, new DataType[]{dt})};
	}

	@Override
	public void close() {
		child.close();
	}

	private double agg_impl(DBColumn[] col, Aggregate agg) {
		if (col == null) {
			return Double.parseDouble(null);
		} else {
			switch (agg) {
				case COUNT:
					double count = 0;
					do{
						count += col[fieldNo].fields.length;
						col = child.next();
					}while(col != null);
					return count;

				case SUM:
					double sum = 0;
					do{
						for(double field: col[fieldNo].getAsDouble()){
							sum = sum + field;
						}
						col = child.next();
					}while(col != null);

					return sum;

				case MIN:
					double min = (double) col[fieldNo].getAsDouble()[0];
					do{
						for(double field: col[fieldNo].getAsDouble()){
							if (field < min) {
								min = field;
							}
						}
						col = child.next();
					}while(col != null);
					return min;

				case MAX:
					double max = col[fieldNo].getAsDouble()[0];
					do{
						for(double field: col[fieldNo].getAsDouble()){
							if ( field > max) {
								max = field;
							}
						}
						col = child.next();
					}while(col != null);

					return max;

				case AVG:
					return agg_impl(col, Aggregate.SUM) / agg_impl(col, Aggregate.COUNT);
			}
			return Double.parseDouble(null);
		}
	}

}
