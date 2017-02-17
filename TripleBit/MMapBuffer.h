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

#ifndef MMAPBUFFER_H_
#define MMAPBUFFER_H_

#include "TripleBit.h"
//#include "MMappedFile.h"
class MMappedFile;

class MMapBuffer {
protected:
	MMappedFile* mfile;
	size_t length;
	char * address;
public:
	char * get_address() const { return (char*)address; }
    char* getBuffer();
    char* getBuffer(size_t pos);

	Status resize(size_t new_size,bool clear);
	size_t get_length() {return length;}
	void   memset(char value);
	Status   flush();
	Status close();

	MMapBuffer(const char* filename, size_t initSize);
	virtual ~MMapBuffer();
	Status remove();
    const char* getName();
public:
	static MMapBuffer* create(const char* filename, size_t initSize);
};

#endif /* MMAPBUFFER_H_ */
