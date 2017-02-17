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

#ifndef _MMAPPED_FILE_H_
#define _MMAPPED_FILE_H_

#include "OSFile.h"
#include <sys/mman.h>

class MMappedFile : public OSFile
{
protected:
#ifdef TRIPLEBIT_WINDOWS
	void*			md;
#endif
	size_t			init_size;
	size_t			mmap_size;
	char *			mmap_addr;

public:
	MMappedFile(const char* name, size_t init_size);
	char*	get_address() {return mmap_addr;}

	virtual ~MMappedFile(void);
	Status	flush();
	Status	open(AccessMode mode, int flags = 0);
	Status	close();
	Status	get_size(size_t& size) const;
	Status	set_size(size_t new_size);
};

#endif
