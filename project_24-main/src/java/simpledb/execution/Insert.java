package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private OpIterator child;
    private int tableId;
    private boolean ifcalled = false;
    private TupleDesc tupleDesc;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.tableId = tableId;
        this.child = child;
        this.tid = t;
        tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */

    //I  thinkk this will work once we'll implement the functions in the bufferPool
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        int insertedRecordsNum = 0;

        // Check if the method is called for the first time
        if (!ifcalled) {
            // Process each tuple from the child operator
            while (child.hasNext()) {
                // Get the next tuple
                Tuple tuple = child.next();

                //Not implemented the deleteTUple yet
                try {
                    Database.getBufferPool().insertTuple(tid, this.tableId,tuple);
                    
                } catch (IOException e) {
                    throw new DbException("Error occurred while deleting tuple: " + e.getMessage());
                }
                insertedRecordsNum++;
            }
            // Mark that the method has been called once
            ifcalled = true;
            Tuple resultTuple = new Tuple(getTupleDesc());
            resultTuple.setField(0, new IntField(insertedRecordsNum));
            return resultTuple;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (children.length > 1)
            child = children[0];
    }
}
