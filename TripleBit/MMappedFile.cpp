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

#include "MMappedFile.h"

MMappedFile::MMappedFile(const char* name, size_t init_size) : OSFile(name)
{
	mmap_addr = NULL;	
	this->init_size = init_size;
}

MMappedFile::~MMappedFile(void)
{
	//printf("MMappedFile address:%x size:%u\n",mmap_addr,mmap_size);
//	munmap(mmap_addr,mmap_size);
}

Status MMappedFile::get_size(size_t& size) const 
{
	if(opened) {
		size = mmap_size;
		return OK;
	} else {
		size = 0;
		return NOT_OPENED;
	}
}

#ifdef TRIPLEBIT_WINDOWS

#include <windows.h>

static int win_page_access[] = {
	PAGE_READONLY, PAGE_READWRITE, PAGE_READWRITE
};

static int win_map_access[] = {
	FILE_MAP_READ, FILE_MAP_WRITE, FILE_MAP_ALL_ACCESS
};

Status MMappedFile::flush()
{
	if (opened) {
		return FlushViewOfFile(mmap_addr, mmap_size)
			? OK : get_system_error();
	} else {
		return NOT_OPENED;
	}
}

Status MMappedFile::open( AccessMode mode, int flags/*=0*/ )
{
	Status status = OSFile::open(mode, flags);
	if (status == OK) {

		LARGE_INTEGER li;
		if(GetFileSizeEx(fd, &li)==0)
			return get_system_error();

		assert( li.QuadPart < SIZE_MAX );

		mmap_size = (li.QuadPart < init_size) ? init_size : li.QuadPart;
		md = CreateFileMapping(fd, NULL,
			win_page_access[mode],
			0, mmap_size, NULL);
		if (md == NULL) {
			status = get_system_error();
			OSFile::close();
			return status;
		}
		mmap_addr = (char*)MapViewOfFile(md, win_map_access[mode], 0, 0, 0);
		if (mmap_addr == NULL) {
			status = get_system_error();
			OSFile::close();
			return status;
		}
	}
	return status;
}

Status MMappedFile::close()
{
	if (opened) {
		if (!UnmapViewOfFile(mmap_addr) || !CloseHandle(md)) {
			return get_system_error();
		}
		return OSFile::close();
	}
	return OK;
}

Status MMappedFile::set_size( size_t new_size )
{
	if (opened) {
		Status status;

		if (new_size > mmap_size) {
			if (!UnmapViewOfFile(mmap_addr) || !CloseHandle(md)) {
				return get_system_error();
			}
			md = CreateFileMapping(fd, NULL,
				win_page_access[mode],
				0, new_size, NULL);
			if (md == NULL) {
				status = get_system_error();
				OSFile::close();
				return status;
			}
			mmap_addr = (char*)MapViewOfFile(md, win_map_access[mode],
				0, 0, 0);
			if (mmap_addr == NULL) {
				status = get_system_error();
				OSFile::close();
				return status;
			}
			mmap_size = new_size;
		}else if(new_size < mmap_size){
			size_t page_size = 8*1024;
			new_size = ALIGN(new_size, page_size);
			if (new_size < mmap_size) {
				if (!UnmapViewOfFile(mmap_addr) || !CloseHandle(md)) {
					return get_system_error();
				}
				LARGE_INTEGER li;
				li.QuadPart = new_size;
				if (SetFilePointerEx(fd, li, NULL, FILE_BEGIN) == 0){
					return get_system_error();
				}
				if( SetEndOfFile(fd) == 0)
					return get_system_error();
				md = CreateFileMapping(fd, NULL,win_page_access[mode],
					0, new_size, NULL);
				if (md == NULL) {
					status = get_system_error();
					OSFile::close();
					return status;
				}
				mmap_addr = (char*)MapViewOfFile(md, win_map_access[mode],
					0, 0, 0);
				if (mmap_addr == NULL) {
					status = get_system_error();
					OSFile::close();
					return status;
				}
				mmap_size = new_size;
			}
		}
		return OK;
	} else {
		return NOT_OPENED;
	}
}

#else // TRIPLEBIT_UNIX

#include <unistd.h>
#include <fcntl.h>
#include <bits/errno.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/uio.h>
	
Status MMappedFile::flush()
{
	if (opened) {
		if (msync(mmap_addr, mmap_size, MS_SYNC) == 0) {
			return OK;
		} else {
			return ERR;
		}
	} else {
		return NOT_OPENED;
	}
}

Status MMappedFile::open( AccessMode mode, int flags/*=0*/ )
{
	Status status = OSFile::open(mode, flags);
	if (status == OK) {
		size_t size = lseek(fd, 0, SEEK_END);
        assert(fd);
		if (size < init_size) {
			size = init_size;
			if (ftruncate(fd, init_size) != 0) {
                status = ERR;
				OSFile::close();
				return status;
			}
		}
		if (lseek(fd, 0, SEEK_SET) != 0) {
			status = ERR;
			OSFile::close();
			return status;
		}
		mmap_addr = (char*)mmap(NULL, size,
			mode == OSFile::F_READ ? PROT_READ :
			mode == OSFile::F_WRITE ? PROT_WRITE :
			PROT_READ|PROT_WRITE,
			MAP_FILE|MAP_SHARED,
			fd, 0);
		if (mmap_addr != MAP_FAILED) {
			mmap_size = size;
			return OK;
		}
        printf("errno :%d mode:%u\t\t%s\n",errno,mode,strerror(errno));
		mmap_addr = NULL;
		status = ERR;
		OSFile::close();
	}
	return status;
}

Status MMappedFile::close()
{
	if (opened) {
		if (munmap(mmap_addr, mmap_size) != 0) {
			return ERR;
		}
		return OSFile::close();
	}
	return OK;
}

Status MMappedFile::set_size( size_t new_size )
{
    int mu = -1,ft = -1;
	if (opened) {
		if (new_size > mmap_size) {
			void* new_addr = NULL;
			if ((mu =munmap(mmap_addr, mmap_size)) != 0
				|| (ft=ftruncate(fd, new_size)) != 0
				|| (new_addr = (char*)mmap(NULL, new_size,
				mode == OSFile::F_READ ? PROT_READ :
				mode == OSFile::F_WRITE ? PROT_WRITE :
				PROT_READ|PROT_WRITE,
				MAP_FILE|MAP_SHARED,
				fd, 0)) == (char*)MAP_FAILED)
			{
                printf("errno :%d mode:%u\t\t%s newsize:%zu fileName:%s\n",errno,mode,strerror(errno),new_size,get_name()); 
                assert((new_addr = (char*)mmap(NULL, new_size,
                PROT_READ|PROT_WRITE,
                MAP_FILE|MAP_SHARED|MAP_NORESERVE,
                fd, 0)) != (char*)MAP_FAILED);
            	return ERR;
			}
			mmap_addr = (char*)new_addr;
		} else if (new_size < mmap_size) {
			size_t page_size = getpagesize();
			new_size = ALIGN(new_size, page_size);
			if (new_size < mmap_size) {
				if (munmap((char*)mmap_addr+new_size, mmap_size-new_size) != 0
					|| ftruncate(fd, new_size) != 0 )
				{
					return ERR;
                    
				}
			}
		}
		mmap_size = new_size;
		return OK;
	} else {
        assert(false);
		return NOT_OPENED;
	}
}

#endif
