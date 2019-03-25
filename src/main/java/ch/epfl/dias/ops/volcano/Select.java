package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;
import com.sun.org.apache.xpath.internal.functions.FuncFalse;

public class Select implements VolcanoOperator {
	private VolcanoOperator child;
	private BinaryOp op;
	private int fieldNo;
	private int value;

	public Select(VolcanoOperator child, BinaryOp op, int fieldNo, int value) {
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
	public DBTuple next() {
		DBTuple tuple = child.next();
		while (!tuple.eof) {
			if (judge(op, tuple.getFieldAsInt(fieldNo), value)) {
				return tuple;
			}
			tuple = child.next();
		}
		System.out.println(tuple.eof);
		return  new DBTuple();
	}

	@Override
	public void close() {
		child.close();
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
