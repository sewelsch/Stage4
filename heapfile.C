#include "heapfile.h"
#include "error.h"

// This function creates an empty (well, almost empty) heap file. To do this create a db level file by calling db->createfile().
// Then, allocate an empty page by invoking bm->allocPage() appropriately. As you know allocPage() will return a pointer to an empty 
// page in the buffer pool along with the page number of the page. Take the Page* pointer returned from allocPage() and cast it to a 
// FileHdrPage*. Using this pointer initialize the values in the header page.  Then make a second call to bm->allocPage(). This page
//  will be the first data page of the file. Using the Page* pointer returned, invoke its init() method to initialize the page contents.  
//  Finally, store the page number of the data page in firstPage and lastPage attributes of the FileHdrPage.

// When you have done all this unpin both pages and mark them as dirty.

// TIPS: If you use a pointer, make sure that pointer has been properly initialized. For example, many students 
// write the first few lines of code for this procedure like this:
//     status = db.openFile(fileName, file);
//     if (status != OK) {
//        status = db.createFile(fileName);
//        status = bufMgr->allocPage(file,hdrPageNo, newPage);
//        ...    
// This is wrong. If status != OK, then the file fileName does not exist, and the variable "file" is not initialized. 
// The command db.createFile(fileName) creates a file named fileName on disk. But you still have to open this file, 
// using db.openFile(fileName, file) again. Or else the variable "file" is still not initialized, and so the next command, 
// bufMgr->allocPage(file, hdrPageNo, newPage) will refer to an uninitialized variable "file", and you will get a segmentation
//  fault somewhere. Remember that all pointers that you use must have been initialized correctly.
// routine to create a heapfile

const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {
		// file doesn't exist. First create it and allocate
		// an empty header page and data page.
		status = db.createFile(fileName);
        if (status != OK){
            return status;
        }
        status = db.openFile(fileName, file);
        if (status != OK){
            return status;
        }
        status = bufMgr->allocPage(file,hdrPageNo, newPage);
        if (status != OK){
            return status;
        }

        // cast newPage * to FileHdrPage*
        hdrPage = (FileHdrPage*) newPage;

        // use hdrPage pointer to init values in header page
        hdrPage->firstPage = 0;
        hdrPage->lastPage = 0;
        hdrPage->pageCnt = 0;
        hdrPage->recCnt = 0;
        // transfer over file name contents to header
        int n = fileName.size();
        for (int i = 0; i < n; i++){
            hdrPage->fileName[i] = fileName[i];
        }
        hdrPage->fileName[n] = "\0"; // end char

        
    
        // make second call to allocPage()
        status = bufMgr->allocPage(file, newPageNo, newPage);
        if (status != OK){
            return status;
        }

        // call init() method
        newPage->init(newPageNo);
        newPage->setNextPage(-1);// shouldn't have next page

        // update firstPage and lastPage attributes
        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;
        // increment page count
        hdrPage->pageCnt += 1;
        
		
        // When you have done all this unpin both pages and mark them as dirty.
		status = bufMgr->unPinPage(file, hdrPageNo, true);
        if (status != OK){
            return status;
        }
        status = bufMgr->unPinPage(file, newPageNo, true);
        if (status != OK){
            return status;
        }
		
        return OK; // TODO: not sure exactly what to return
		
    }
    return (FILEEXISTS);
}

// This is easy. Simply call db->destroyFile(). The user is expected to have closed 
// all instances of the file before calling this function.

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

// This method first opens the appropriate file by calling db.openFile() (do not forget to save 
// the File* returned in the filePtr data member). Next, it reads and pins the header page for the file 
// in the buffer pool, initializing the private data members headerPage, headerPageNo, and hdrDirtyFlag. 
// You might be wondering how you get the page number of the header page. This is what file->getFirstPage() 
// is used for (see description of the I/O layer)! Finally, read and pin the first page of the file into the
//  buffer pool, initializing the values of curPage, curPageNo, and curDirtyFlag appropriately. Set curRec 
//  to NULLRID.

// constructor opens the underlying file
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        File* filePointer = filePtr;

        // initializing headerPage, headerPageNo, hdrDirtyFlag
        headerPage = NULL;
        int headerPageNo = 0;
        status = filePointer->getFirstPage(headerPageNo); // Where to get page number?
        if (status != OK){
            returnStatus = status;
            return;
        }
        hdrDirtyFlag = false;
                
        // reads and pins the header page for the file 
        status = bufMgr->readPage(filePtr, headerPageNo, pagePtr);
        if (status != OK){
            returnStatus = status;
            return;
        }
        headerPage = (FileHdrPage*) pagePtr;
        
        // read and pin first page of file
        int firstPage = headerPage->firstPage;
        status = bufMgr->readPage(filePtr, firstPage, pagePtr);

        if (status != OK){
            returnStatus = status;
            return;
        }

        curPage = pagePtr;
        curPageNo = firstPage;
        curDirtyFlag = false;
        curRec = NULLRID;
        
        // if everything goes well, status == OK
        returnStatus = OK;

        return;
		
    }
    else
    {
    	cerr << "open of heap file failed\n";
		returnStatus = status;
		return;
    }
}

// The destructor first unpins the header page and currently pinned data page and then calls db.closeFile.

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }
	
	 // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
	
	// status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
	// if (status != OK) cerr << "error in flushFile call\n";
	// before close the file
	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}

// Returns the number of records currently in the file (as found in the header page for the file).

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

// This method returns a record (via the rec structure) given the RID of the record. The private data members curPage and 
// curPageNo should be used to keep track of the current data page pinned in the buffer pool. If the desired record is on 
// the currently pinned page, simply invoke
// curPage->getRecord(rid, rec) to get the record.  Otherwise, you need to unpin the currently pinned page 
// (assuming a page is pinned) and use the pageNo field of the RID to read the page into the buffer pool.

// TIPS: In heapfile.C the comment above this method says it "retrieves an arbitrary record from a file". This should be understood 
// as retrieving an arbitrary record given the RID of the record. This does not mean just take a random record from the 
//file and return that.

// Further, you should check if curPage is NULL. If yes, you need to read the right page (the one with the requested record 
// on it) into the buffer. Make sure when you do this that you DO THE BOOKKEEPING. That is, you need to set the fields 
// curPage, curPageNo, curDirtyFlag, and curRec of the HeapFile object appropriately. Do not forget to do bookkeeping, 
// or else your code won't work properly.

// TODO 

const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;

    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
   
    // if curpage not null and curpageno = rid.pageNo, record is on current page
    if (curPage != NULL && curPageNo == rid.pageNo){
        // invoke curPage->getRecord(rid,rec)
        status  = curPage->getRecord(rid, rec);
        curRec = rid;
        curDirtyFlag = false;

        return status;
    }
    
    // elif record on diff page
    else if (curPage != NULL) {
        // unpin current page
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK){
            return status;
        }
    }

    // read and pin current record's page
    status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
    if (status != OK){
        return status;
    }

    // update variables
    curPageNo = rid.pageNo;
    curDirtyFlag = false;
    curRec = rid;

    status = curPage->getRecord(rid, rec);
    return status;
}

// The HeapFile class primarily deals with pages.To insert records into a file, the InsertFileScan class is used (described below).
// To retrieve records the HeapFileScan class (which also represents a HeapFile since it is derived from the HeapFile class) is 
// used.  The HeapFileScan class can be used in three ways:

// 1.       To retrieve all records from a HeapFile.

// 2.       To retrieve only those records that match a specified predicate.

// 3.       To delete records in a file

// Several HeapFileScans may be instantiated on the same file simultaneously.This will work fine as long as each has its own 
// "current" FileHdrPage pinned in the buffer pool. Note that the HeapFileScan class is derived from the HeapFile class, thus 
// inheriting its data members and member functions.

// Initializes the data members of the object and then opens the appropriate heapfile by calling the HeapFile constructor
//  with the name of the file. The status of all these operations is indicated via the status parameter.

 

// For those of you new to C++, since HeapFileScan is derived from HeapFile, the constructor for the HeapFile class is 
// invoked before the HeapFileScan constructor is invoked.
// Shuts down the scan by calling endScan().  After the HeapFileScan destructor is invoked, the HeapFile destructor 
// will be automatically invoked

HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

// This method initiates a scan over a file. If filter == NULL, an unconditional scan is performed meaning that the scan
//  will return all the records in the file. Otherwise, the data members of the HeapFileScan object are initialized with 
// the parameters to the method.

const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}

// This method terminates a scan over a file but does not delete the scan object. 
// This will allow the scan object to be reused for another scan.

const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

// Saves the current position of the scan by preserving the values of curPageNo and 
// curRec in the private data members markedPageNo and markedRec, respectively.

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

// Resets the position of the scan to the position when the scan was last marked by restoring the values of 
// curPageNo and curRec from markedPageNo and markedRec, respectively. Unless the page number of the currently pinned 
// page is the same as the marked page number, unpin the currently pinned page, then read markedPageNo from disk and set 
// curPageNo, curPage, curRec, and curDirtyFlag appropriately.

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}

// Returns (via the outRid parameter) the RID of the next record that satisfies the scan predicate. The basic idea is to scan 
//the file one page at a time. For each page, use the firstRecord() and nextRecord() methods of the Page class to get the rids 
// of all the records on the page. Convert the rid to a pointer to the record data and invoke matchRec() to determine if record 
// satisfies the filter associated with the scan. If so, store the rid in curRec and return curRec. To make things fast, keep the 
// current page pinned until all the records on the page have been processed. Then continue with the next page in the file.  
//Since the HeapFileScan class is derived from the HeapFile class it also has all the methods of the HeapFile class as well. 
//Returns OK if no errors occurred. Otherwise, return the error code of the first error that occurred.

// TODO
const Status HeapFileScan::scanNext(RID& outRid)
{
    Status 	status = OK;
    RID		nextRid;
    RID		tmpRid;
    int 	nextPageNo;
    Record      rec;
    // return rid of next record that satisfies the scan
    // if no current page, get it to be able to pin it
    if (curPage == NULL){
        // get curPage
        // TODO
        
        
    }
    
    

    
	
	
	
	
	
	
	
	
	
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
// Inserts the record described by rec into the file returning the RID of the 
// inserted record in outRid.

//TIPS: check if curPage is NULL. If so, make the last page the current page and read it into
// the buffer. Call curPage->insertRecord to insert the record. If successful, remember to
//  DO THE BOOKKEEPING. That is, you have to update data fields such as recCnt, hdrDirtyFlag,
// curDirtyFlag, etc.

//If can't insert into the current page, then create a new page, initialize it properly, 
// modify the header page content properly, link up the new page appropriately, make the
// current page to be the newly allocated page, then try to insert the record. Don't forget 
//bookkeeping that must be done after successfully inserting the record.


const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage;
    int		newPageNo;
    Status	status, unpinstatus;
    RID		rid;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    
    
    

  
  
  
  
  

  
  
  
}