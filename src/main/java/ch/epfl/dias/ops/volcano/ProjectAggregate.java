package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class ProjectAggregate implements VolcanoOperator {

	private VolcanoOperator child;
	private Aggregate agg;
	private DataType dt;
	private int fieldNo;

	public ProjectAggregate(VolcanoOperator child, Aggregate agg, DataType dt, int fieldNo) {
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
	public DBTuple next() {
		Object[] field = new Object[1];
		DataType[] dts = new DataType[] {dt};
		field[0] =  agg_impl(agg);
		return (new DBTuple(field, dts));
	}

	@Override
	public void close() {
		child.close();
	}

	private double agg_impl(Aggregate agg){
		DBTuple tuple = child.next();
		switch (agg) {
			case COUNT:
				double count = 0;
				while(!tuple.eof){
					if (tuple.getFieldAsDouble(fieldNo) != null) {
						count = count + 1;
					}
					tuple = child.next();
				}
				return count;

			case SUM:
				double sum = 0;
				while(!tuple.eof){
					if (tuple.getFieldAsDouble(fieldNo) != null) {
						sum = sum + tuple.getFieldAsDouble(fieldNo);
					}
					tuple = child.next();
				}
				return sum;

			case MIN:
				double min = tuple.getFieldAsDouble(fieldNo);
				while(!tuple.eof) {
					double value = tuple.getFieldAsDouble(fieldNo);
					if (value < min) {
						min = value;
					}
					tuple = child.next();
				}
				return min;

			case MAX:
				double max = tuple.getFieldAsDouble(fieldNo);
				while(!tuple.eof) {
					double value = tuple.getFieldAsDouble(fieldNo);
					if (value > max) {
						max = value;
					}
					tuple = child.next();
				}
				return max;

			case AVG:
				double sum_ = 0;
				double count_ = 0;
				while(!tuple.eof){
					if (tuple.getFieldAsDouble(fieldNo) != null) {
						sum_ = sum_ + tuple.getFieldAsDouble(fieldNo);
						count_++;
					}
					tuple = child.next();
				}
				return sum_/count_;
		}
		return Double.parseDouble(null);
	}

}
