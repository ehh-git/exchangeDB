package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */

public class HeapFile implements DbFile {
    // newly added
    private final File f;
    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td= td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    // bruh go back and fix
    public Page readPage(PageId pid) {
        try {
            RandomAccessFile raf = new RandomAccessFile(f, "r");
            byte[] data = new byte[BufferPool.getPageSize()];
            long offset = pid.getPageNumber() * BufferPool.getPageSize();
            raf.seek(offset);
            raf.readFully(data);
            raf.close();
            return new HeapPage((HeapPageId)pid, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        long fileSize = f.length();
        return (int) Math.ceil((double) fileSize / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    // It doesn't insert the righ amount - its +1 to what it's meant to be
    public List<Page> insertTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        List<Page> modifiedPages = new ArrayList<>();

        int numPages = numPages();

    for (int pageNo = 0; pageNo < numPages; pageNo++) {
        // Create a new HeapPageId for the current page
        HeapPageId pid = new HeapPageId(getId(), pageNo);

        // Read the page from disk
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);

        // Try to insert the tuple into the page
        try {
            page.insertTuple(t);
            // Mark the page as dirty and add it to the list of modified pages
            //page.markDirty(true, tid);
            modifiedPages.add(page);
            // Break the loop as the tuple has been successfully inserted
            break;
        } catch (DbException e) {
            // If insertion fails due to lack of space, continue to the next page
            if (e.getMessage().equals("No space left for tuple")) {
                continue;
            } else {
                // If insertion fails due to other reasons, re-throw the exception
                throw e;
            }
        }
    }

    // If no page had enough space, create a new page and insert the tuple into it
    if (modifiedPages.isEmpty()) {
        HeapPageId newPageId = new HeapPageId(getId(), numPages);
        HeapPage newPage = new HeapPage(newPageId, HeapPage.createEmptyPageData());
        newPage.insertTuple(t);
        // Mark the new page as dirty and add it to the list of modified pages
        //newPage.markDirty(true, tid);
        modifiedPages.add(newPage);
    }

    return modifiedPages;
}


    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    // ????
    // made a new file, is that ok?
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this.getId(), tid, this.numPages());
    }

}