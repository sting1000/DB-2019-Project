package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

import java.util.*;
//import java.util.stream.Stream;

public class HashJoin implements VolcanoOperator {
	private VolcanoOperator leftChild;
	private VolcanoOperator rightChild;
	private int leftFieldNo;
	private int rightFieldNo;
	private Hashtable<Integer, ArrayList<DBTuple>> table;
	private ArrayList<DBTuple> match;
	private DBTuple currentTuple;
	private ArrayList<DBTuple> wait;
	int wait_pos = 0;


	public HashJoin(VolcanoOperator leftChild, VolcanoOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
		this.table = new Hashtable<Integer, ArrayList<DBTuple>>();
		wait = new ArrayList<>();
	}

	@Override
	public void open() {
		leftChild.open();
		rightChild.open();
		DBTuple tuple1 = leftChild.next();
		while (!tuple1.eof) {
			buildHashTable(tuple1, leftFieldNo);
			tuple1 = leftChild.next();
		}
		currentTuple = rightChild.next();
	}

	@Override
	public DBTuple next() {
		while (!currentTuple.eof) {
			if (probe(currentTuple, rightFieldNo)) {
				for (DBTuple tup_tmp : match) {
					wait.add(join(tup_tmp, currentTuple));
				}
			}
			currentTuple = rightChild.next();
		}
		if (wait.size() > wait_pos) {
			wait_pos++;
			return wait.get(wait_pos - 1);
		}
		return new DBTuple();
	}

	@Override
	public void close() {
		leftChild.close();
		rightChild.close();
	}

	public void buildHashTable(DBTuple t, int FieldNo){
		Integer fieldValue = t.getFieldAsInt(FieldNo);
		if (fieldValue != null) {
			try {
				table.get(fieldValue).add(t);
			} catch (NullPointerException e) {
				table.put(fieldValue, new ArrayList<DBTuple>());
				table.get(fieldValue).add(t);
			}
		}
	}
	public boolean probe(DBTuple t, int FieldNo) {
		match = table.get(t.getFieldAsInt(FieldNo));
		return (match != null);
	}

	public DBTuple join(DBTuple t1, DBTuple t2){
		Object[] field = new Object[t1.fields.length + t2.fields.length];
		DataType[] type= new DataType[t1.types.length + t2.types.length];
		int i;
		for (i = 0; i < t1.fields.length; i++) {
			field[i] = t1.fields[i];
			type[i] = t1.types[i];
		}
		for (int j = i; j < i + t2.fields.length; j++) {
			field[j] = t2.fields[j-i];
			type[j] = t2.types[j-i];
		}
		return new DBTuple(field, type);
	}
}
