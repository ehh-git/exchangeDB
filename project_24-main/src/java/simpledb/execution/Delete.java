package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private OpIterator child;
    private boolean ifcalled = false;
    private TupleDesc tupleDesc;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.tid = t;
        this.child = child;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */

    //This should also work once we have bufferPool functions working
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        int deletedRecordsCount = 0;

        // Check if the method is called for the first time
        if (!ifcalled) {
            // Process each tuple from the child operator
            while (child.hasNext()) {
                // Get the next tuple
                Tuple tuple = child.next();

                //Not implemented the deleteTUple yet
                try {
                    Database.getBufferPool().deleteTuple(tid, tuple);
                    
                } catch (IOException e) {
                    throw new DbException("Error occurred while deleting tuple: " + e.getMessage());
                }
                deletedRecordsCount++;
            }
            // Mark that the method has been called once
            ifcalled = true;
            // Create a new tuple containing the count of deleted records
            Tuple resultTuple = new Tuple(getTupleDesc());
            resultTuple.setField(0, new IntField(deletedRecordsCount));
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
