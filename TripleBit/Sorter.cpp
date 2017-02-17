//---------------------------------------------------------------------------
// TripleBit
// (c) 2011 Massive Data Management Group @ SCTS & CGCL. 
//     Web site: http://grid.hust.edu.cn/triplebit
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------

#include "Sorter.h"
#include "TempFile.h"
#include "MMapBuffer.h"
#include <vector>
#include <algorithm>
#include <cassert>
#include <cstring>
using namespace std;
//---------------------------------------------------------------------------
/// Maximum amount of usable memory. XXX detect at runtime!
static const unsigned memoryLimit = sizeof(void*) * (1 << 28);
int (*coupleCompare)(const char*, const char*);
static const unsigned maxThreadSortRun =32;
static const unsigned MAX_THREAD_NUM = 8;
//MutexLock Sorter::flushLock = MutexLock();
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
/// A memory range

//---------------------------------------------------------------------------
/// Sort wrapper that colls the comparison function
struct CompareSorter {
    /// Comparison function
    typedef int (*func)(const char*, const char*);

    /// Comparison function
    const func compare;

    /// Constructor
    CompareSorter(func compare) :
        compare(compare) {
    }

    /// Compare two entries
    bool operator()(const Range& a, const Range& b) const {
        return compare(a.from, b.from) < 0;
    }
};
//---------------------------------------------------------------------------
static void spool(TempFile& out, const vector<Range>& items) { // Spool items to disk
    for(vector<Range>::const_iterator iter = items.begin(); iter != items.end(); ++iter) {
        out.write(iter->to - iter->from, iter->from);
    }
    return;
}

static void spool(const vector<Range>& items,MMapBuffer* out,size_t offset) { // Spool items to disk
    char* dest = NULL;
    size_t lastoffset = offset;
    
    for(vector<Range>::const_iterator iter = items.begin(); iter != items.end(); ++iter) {
          dest = out->get_address() + offset;
          memcpy(dest, iter->from, iter->to - iter->from);
          offset += (iter->to - iter->from);
    }

    out->flush();
    /*    if(lastoffset + 8*items.size() != offset){
    	cout <<lastoffset <<"  " << 8*items.size() <<"  " << offset << endl;
    }
   assert(lastoffset + 8*items.size() == offset);

    Sorter::flushLock.lock();
    out->flush();
    size_t head = lastoffset% MemoryBuffer::pagesize;
    head = msync((void*)(out->get_address()+lastoffset -head),8*items.size()+head, MS_SYNC);
    assert(head == 0);
    Sorter::flushLock.unLock();*/
//    cout <<"spool:"<<lastoffset <<"  " << offset << endl;
    return;
}



/* sortRun() thread wrapper */
struct SortRunWrapperArgs {
    pthread_t m_thread;
    std::vector<Range> m_items;
    int (*m_compare)(const char*, const char*);
    char* m_ofs;
    TempFile* m_out;
    size_t m_offset;
    MMapBuffer* m_outchar;
    SortRunWrapperArgs(std::vector<Range>& items, int (*compare)(const char*, const char*), TempFile* out):
        m_thread(-1),
        m_items(items),
        m_compare(compare),
        m_out(out),
        m_outchar(NULL)
    {
        pthread_create(&m_thread, NULL, (void*(*)(void*))SortRun, this);
    }

    SortRunWrapperArgs(std::vector<Range>& items, int (*compare)(const char*, const char*), MMapBuffer* out,size_t offset):
         m_thread(-1),
         m_items(items),
         m_compare(compare),
         m_outchar(out),
         m_offset(offset),
         m_out(NULL)
   {
         pthread_create(&m_thread, NULL, (void*(*)(void*))SortRun, this);
   }


    ~SortRunWrapperArgs() {
        pthread_join(m_thread, NULL);
        m_thread = -1;
    }
    static bool checkSort(vector<Range>& items,int begin,int end){
    	for(unsigned i = begin+1; i <=end; i++){
    		if(coupleCompare(items[i-1].from,items[i].from) >0){
    			cout<<"check sort:" <<*(ID*)items[i-1].from <<"=" <<*(ID*)(items[i-1].from+4)<< endl;
    			cout<<"check sort:" <<*(ID*)items[i].from <<"=" <<*(ID*)(items[i].from+4)<< endl;
    			return false;
    		}
    	}
    	return true;
    }

    static bool printSort(vector<Range>& items,int low,int high){
    	for(unsigned i = 0; i <= high; i++){
    		cout <<*(ID*)items[i].from <<"=" << *(ID*)(items[i].from+4) << " "<< flush;
    	}
    	cout << endl;
    }

   static int Partition(vector<Range>& items, int i,int j){
	   Range pivot = items[i];
	   while(i<j){
		   while(i<j && coupleCompare(items[j].from, pivot.from) >= 0)
			   j--;
		   if(i<j){
			   items[i].from = items[j].from;
			   items[i].to = items[j].to;
			   i++;
		   }
		   while(i<j &&coupleCompare(items[i].from, pivot.from)<= 0 )
			   i++;
		   if(i <j){
			   items[j].from = items[i].from;
			   items[j].to = items[i].to;
			   j--;
		   }
	   }
	   items[i].from = pivot.from;
	   items[i].to = pivot.to;
	   return i;
   }
   static void quickSort(vector<Range>& items,int low,int high){
	   int pos;
    	if (low < high) {
			pos = Partition(items,low,high);
			quickSort(items,low,pos-1);
			quickSort(items,pos+1,high);
		}
    }

   static void insertSort(vector<Range>& items) {
	   unsigned len = items.size();
		for (int i = 1; i < len; i++) {
			Range t = items[i];
			int j = i;
			while ((j > 0) &&coupleCompare(items[j-1].from, t.from) >= 0 ) {
				items[j] = items[j - 1];
				--j;
			}
			items[j] = t;

		}

	}

    static inline void SortRun(SortRunWrapperArgs* args) {
 //   	cout <<"begin quick sort" << endl;
    	std::sort(args->m_items.begin(), args->m_items.end(), CompareSorter(args->m_compare));

        if(args->m_out)
            spool(*args->m_out, args->m_items);
        else
            spool(args->m_items,args->m_outchar,args->m_offset);

    }
};
//---------------------------------------------------------------------------
}

#define DEBUG_TIMER 1
//#include "DebugTimer.h"
//---------------------------------------------------------------------------
void Sorter::sort(TempFile& in, TempFile& out, const char* (*skip)(const char*), int(*compare)(const char*, const char*))
    // Sort a temporary file
{
 //   DebugTimer timer("DebugTimer: Sorter::sort() = %.2fs\n");
 //   DebugTimerObject _(timer);

    // Open the input
	coupleCompare = compare;
    in.close();
    MemoryMappedFile mappedIn;
    assert(mappedIn.open(in.getFile().c_str()));
//    assert(mappedIn.open("/jpkc/01/Data/yahoo/couple"));
    const char* reader = mappedIn.getBegin(),*begin= reader, *limit = mappedIn.getEnd();
    const char* ofs = 0;

    SortRunWrapperArgs* args[maxThreadSortRun];
    int index = 0;
    for(unsigned i = 0; i<maxThreadSortRun; i++)
    	args[i] = NULL;

    // Produce runs
    vector<Range> runs;
    vector<Range> items;
    TempFile intermediate(out.getBaseFile());
    MMapBuffer* mapBuffer = new MMapBuffer(intermediate.getFile().c_str(),limit-begin);
    const char* lastreader = reader;
    while (reader < limit) {
        const char* start = reader;
        const char* maxReader = reader + memoryLimit;
        // Collect items
        while (reader < limit) {
            items.push_back(Range(reader, reader + 8));
            reader = skip(reader);

            // Memory Overflow?
            if (reader + sizeof(Range) * items.size() > maxReader) {
                break;
            }
        }

        // Sort the run
        // std::sort(items.begin(), items.end(), CompareSorter(compare));
        delete args[index];


        if(reader == limit && runs.empty())
            args[index] = new SortRunWrapperArgs(items, compare, &out);
        else
            args[index] = new SortRunWrapperArgs(items, compare, mapBuffer,start - begin);

        index += 1;
        index %= maxThreadSortRun;

        lastreader = reader;

        if(reader == limit && runs.empty()) {
            break;
        }
        runs.push_back(Range(ofs, ofs + 8 * items.size()));
        ofs += 8 * items.size();
        items.clear();
    }
    for(index = 0; index < maxThreadSortRun; index++) { /* join all threads */
        delete args[index];
    }
    intermediate.close();
    mappedIn.close();
    mapBuffer->close();
    delete mapBuffer;

    fprintf(stderr, "Sorter::sort.runs = %d\n", runs.size());

    //flush(runs,&intermediate,&out);
    // Do we habe to merge runs?
   if (!runs.empty()) {
        // Map the ranges
        MemoryMappedFile tempIn;
        assert(tempIn.open(intermediate.getFile().c_str()));
        for (vector<Range>::iterator iter = runs.begin(), limit = runs.end(); iter != limit; ++iter) {
            (*iter).from = tempIn.getBegin() + ((*iter).from - static_cast<char*> (0));
            (*iter).to = tempIn.getBegin() + ((*iter).to - static_cast<char*> (0));
        }

        // Sort the run heads
        std::sort(runs.begin(), runs.end(), CompareSorter(compare));

        // And merge them
        Range last(0, 0);
        while (!runs.empty()) {
            // Write the first entry
            Range head(runs.front().from, skip(runs.front().from));
            out.write(head.to - head.from, head.from);
            last = head;

            // Update the first entry. First entry done?
            if ((runs.front().from = head.to) == runs.front().to) {
                runs[0] = runs[runs.size() - 1];
                runs.pop_back();
            }

            // Check the heap condition
            unsigned pos = 0, size = runs.size();
            while (pos < size) {
                unsigned left = 2 * pos + 1, right = left + 1;
                if (left >= size)
                    break;
                if (right < size) {
                    if (compare(runs[pos].from, runs[left].from) > 0) {
                        if (compare(runs[pos].from, runs[right].from) > 0) {
                            if (compare(runs[left].from, runs[right].from) < 0) {
                                std::swap(runs[pos], runs[left]);
                                pos = left;
                            } else {
                                std::swap(runs[pos], runs[right]);
                                pos = right;
                            }
                        } else {
                            std::swap(runs[pos], runs[left]);
                            pos = left;
                        }
                    } else if (compare(runs[pos].from, runs[right].from) > 0) {
                        std::swap(runs[pos], runs[right]);
                        pos = right;
                    } else
                        break;
                } else {
                    if (compare(runs[pos].from, runs[left].from) > 0) {
                        std::swap(runs[pos], runs[left]);
                        pos = left;
                    } else
                        break;
                }
            }
        }
        tempIn.close();
    }
    intermediate.discard();
    out.close();
}

string getTempName(const string& str,int index){
	std::ostringstream ss;
	return static_cast<std::ostringstream&>(ss << str.c_str() << "_" << index).str();
}

void Sorter::flush(vector<Range>& runs, TempFile* in, TempFile* out){
	size_t k = 32;
	size_t i;
	size_t index = 0;
	std::vector<Range> inRange;
	unsigned m_numBlocks = runs.size();
	int fileFlag = 0;

	MergeRunWrapperArgs* m_mergeRunArgs[MAX_THREAD_NUM];
	for(unsigned i = 0; i<MAX_THREAD_NUM;i++)
		m_mergeRunArgs[i] = NULL;

	unsigned size = 0;
	size_t insize = 0;
	MMapBuffer* m_in = new MMapBuffer(in->getFile().c_str(),0);
	insize = m_in->get_length();
	MMapBuffer* m_out = new MMapBuffer(getTempName(in->getFile(),fileFlag).c_str()
			,insize);
	fileFlag = (fileFlag == 0? 1:0);

	for (vector<Range>::iterator iter = runs.begin(), limit = runs.end(); iter!= limit; ++iter) {
		(*iter).from =m_in->get_address() + ((*iter).from - static_cast<char*> (0));
		(*iter).to = m_in->get_address() + ((*iter).to - static_cast<char*> (0));
	}

	vector<Range> tempRun;
	unsigned tempRunCount;
	while (runs.size() > 1) {
		tempRunCount = runs.size()/k + ((runs.size()%k)?1:0);
		tempRun = vector<Range>(tempRunCount,Range(0,0));
		tempRunCount = 0;
		for (unsigned i = 0; i < runs.size();) {
			size = (i + k < runs.size()) ? k : runs.size() - i;
			delete m_mergeRunArgs[index];
//			cout <<"small run:" << runs.size() <<"  size:" << size <<"  index:" << index<< endl;
			for (unsigned j = 0; j < size; j++)
				inRange.push_back(runs[i + j]);
			if(runs.size() >k){
				m_mergeRunArgs[index] = new MergeRunWrapperArgs(m_in,inRange,m_out,tempRun[tempRunCount++]);
			}
			else{
				m_out->remove();
				delete m_out;

				m_out = new MMapBuffer(out->getFile().c_str(),insize);
				m_mergeRunArgs[index] = new MergeRunWrapperArgs(m_in,inRange,m_out,tempRun[tempRunCount++]);
			}
			inRange.clear();

			i += k;
			index += 1;
			index  = index % MAX_THREAD_NUM;
		}
		for (index = 0; index < MAX_THREAD_NUM; index++) { /* synchronize all m_threads */
			delete m_mergeRunArgs[index];
			m_mergeRunArgs[index] = NULL;
		}
		//cheak outRange
		index = 0;

		m_out->flush();
		runs = tempRun;
		tempRun.clear();

		//check runs
		for(unsigned i = 0; i< runs.size(); i++){
			assert(runs[i].from && runs[i].to);
		}

		if(runs.size() >1){
			delete m_in;
			m_in = m_out;
			m_out = new MMapBuffer(
				getTempName(in->getFile(), fileFlag).c_str(),insize);
			fileFlag = (fileFlag == 0 ? 1 : 0);
		}
	}
	/*assert(runs.size() == 1);
	assert(checkRight(runs[0].from,runs[0].to));*/
	m_in->remove();
	delete m_in;
	delete m_out;


	return;
}

bool Sorter::checkRight(const char* reader,const char *limit){
	while(reader +8< limit){
		if(coupleCompare(reader,reader+8) >0){
			cout << "wrong-"<<*(ID*)reader <<"-"<< *(ID*)(reader+4) << endl;
			reader+=8;
			cout << "wrong-"<<*(ID*)reader <<"-"<< *(ID*)(reader+4) << endl;
			return false;
		}
		else
			reader += 8;
	}
	return true;
}

bool Sorter::printCouple(const char* reader,const char *limit){
	while(reader < limit){
		cout << "-"<<*(ID*)reader <<"-"<< *(ID*)(reader+4) << endl;
		reader += 8;
	}
}

struct PairComparator {
    inline bool operator()(const std::pair<const char*, int>& a, const std::pair<const char*, int>& b) {
    	return coupleCompare(a.first,b.first) >0;
    }
};
void Sorter::mergeRun(MMapBuffer* in, vector<Range>& inRange, MMapBuffer* out, Range& outRange ){

	std::vector<size_t> finpos;
	outRange.from =out->get_address() +( inRange[0].from - in->get_address());
	outRange.to = outRange.from;
	char* writer = (char*)outRange.from;
	size_t minpos = -1;

	std::priority_queue<std::pair<const char*, int>, std::vector<std::pair<const char*, int> >, PairComparator> strs;
	for(unsigned i = 0; i < inRange.size(); i++) {
		strs.push(std::make_pair(inRange[i].from, i));
		outRange.to += (inRange[i].to - inRange[i].from);
		finpos.push_back(8);
	}

	 while (!strs.empty()) {

		memcpy(writer, strs.top().first, 8);
		writer += 8;

		minpos = strs.top().second;
		strs.pop();

		if (finpos[minpos] < (inRange[minpos].to - inRange[minpos].from)) {
			strs.push(std::make_pair(inRange[minpos].from+finpos[minpos],minpos));
			finpos[minpos] += 8;
		}

	}
	if(writer != outRange.to){
		printf("%p  %p\n",writer,outRange.to);
	}
	assert(writer == outRange.to);

	/*flushLock.lock();
	//assert(out->flush() == OK);
	size_t head = (outRange.from - out->get_address())% MemoryBuffer::pagesize;
	int head1 = msync((void*)(outRange.from -head),outRange.from - outRange.to+head, MS_SYNC);
	if(head1){

		cout <<"head:" <<head1 <<" "<< head <<
				"  head offset:" << (outRange.from - out->get_address() -head)
				<<"  len:" <<  (outRange.from - outRange.to+head) << endl;
	}
	assert(head1 == 0);
	flushLock.unLock();*/

/*	if ( checkRight(outRange.from, writer) == false) {

		cout << strs.size() << "--------------------------------" << endl;
		printCouple(outRange.from, writer);

		//			for(unsigned i = 0; i <)
		exit(-1);
	}*/
	//
	return;
}
//---------------------------------------------------------------------------
void Sorter::mergeRun1(MMapBuffer* in, vector<Range>& runs, MMapBuffer* out, Range& outRange ){
	if (!runs.empty()) {
		// Map the ranges
		outRange.from = out->get_address() + (runs[0].from- in->get_address());
		outRange.to = outRange.from;
		char* writer = (char*)outRange.from;
		for (unsigned i = 0; i < runs.size(); i++) {
			outRange.to += (runs[i].to - runs[i].from);
		}
		// Sort the run heads
		std::sort(runs.begin(), runs.end(), CompareSorter(coupleCompare));

		// And merge them
		Range last(0, 0);
		while (!runs.empty()) {
			// Write the first entry
			Range head(runs.front().from, runs.front().from + 8);
			memcpy(writer, head.from, 8);
			writer += 8;
			last = head;

			// Update the first entry. First entry done?
			if ((runs.front().from = head.to) == runs.front().to) {
				runs[0] = runs[runs.size() - 1];
				runs.pop_back();
			}

			// Check the heap condition
			unsigned pos = 0, size = runs.size();
			while (pos < size) {
				unsigned left = 2 * pos + 1, right = left + 1;
				if (left >= size)
					break;
				if (right < size) {
					if (coupleCompare(runs[pos].from, runs[left].from) > 0) {
						if (coupleCompare(runs[pos].from, runs[right].from) > 0) {
							if (coupleCompare(runs[left].from, runs[right].from)
									< 0) {
								std::swap(runs[pos], runs[left]);
								pos = left;
							} else {
								std::swap(runs[pos], runs[right]);
								pos = right;
							}
						} else {
							std::swap(runs[pos], runs[left]);
							pos = left;
						}
					} else if (coupleCompare(runs[pos].from, runs[right].from)
							> 0) {
						std::swap(runs[pos], runs[right]);
						pos = right;
					} else
						break;
				} else {
					if (coupleCompare(runs[pos].from, runs[left].from) > 0) {
						std::swap(runs[pos], runs[left]);
						pos = left;
					} else
						break;
				}
			}
		}
	}
}
