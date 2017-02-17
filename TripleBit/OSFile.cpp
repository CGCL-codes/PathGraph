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

#include "OSFile.h"


OSFile::OSFile(const char * name) : fd(0) {
	// TODO Auto-generated constructor stub
	opened = false;
	this->name[0] = 0;
	size_t len = strlen(name);

	strcpy(this->name, name);
}

OSFile::~OSFile() {
	// TODO Auto-generated destructor stub
	close();
}

char const * OSFile::get_name() const 
{
	return name;
}

#ifdef TRIPLEBIT_WINDOWS

#include <windows.h>
#include <direct.h>

static int win_access_mode[] = {
	GENERIC_READ, GENERIC_WRITE, GENERIC_READ | GENERIC_WRITE
};

static int win_open_flags[] = {
	OPEN_EXISTING, TRUNCATE_EXISTING, OPEN_ALWAYS, CREATE_ALWAYS
};

static int win_open_attrs[] = {
	FILE_FLAG_SEQUENTIAL_SCAN,
	FILE_FLAG_SEQUENTIAL_SCAN|FILE_FLAG_WRITE_THROUGH,
	FILE_FLAG_RANDOM_ACCESS,
	FILE_FLAG_RANDOM_ACCESS|FILE_FLAG_WRITE_THROUGH,
};

static int win_page_access[] = {
	PAGE_READONLY, PAGE_READWRITE, PAGE_READWRITE
};

static int win_map_access[] = {
	FILE_MAP_READ, FILE_MAP_WRITE, FILE_MAP_ALL_ACCESS
};

Status OSFile::get_system_error()
{
	int error = GetLastError();
	switch (error) {
	case ERROR_HANDLE_EOF:
		return END_OF_FILE;
	case NO_ERROR:
		return OK;
	}
	return Status(error);
}

void OSFile::get_error_text( Status code, char* buf, size_t buf_size ) const
{
	int len;
	switch (code) {
	case OK:
		strncpy(buf, "OK", buf_size);
		break;
	case NOT_OPENED:
		strncpy(buf, "file not opened", buf_size);
		break;
	case END_OF_FILE:
		strncpy(buf, "operation not completely finished", buf_size);
		break;
	default:
		len = FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM,
			NULL,
			code,
			0,
			buf,
			buf_size,
			NULL);
		if (len == 0) {
			char errcode[64];
			snprintf(errcode, 64, "unknown error code %u", code);
			strncpy(buf, errcode, buf_size);
		}
	}
}

Status OSFile::set_position( size_t pos )
{
	if (opened) {
		LARGE_INTEGER li;
		li.QuadPart = pos;
		if (SetFilePointerEx(fd, li, NULL, FILE_BEGIN) == 0){
			return get_system_error();
		}
		return OK;
	}
	return NOT_OPENED;
}

Status OSFile::get_position( size_t& pos )
{
	if(opened){
		LARGE_INTEGER lpos;
		LARGE_INTEGER x;
		x.QuadPart = 0;
		if (SetFilePointerEx(fd,x,&lpos,FILE_BEGIN) ==0 ){
			return get_system_error();
		}
	
		assert( lpos.QuadPart < SIZE_MAX );
		pos = lpos.QuadPart;
	
		return OK;
	}
	return NOT_OPENED;
}

Status OSFile::read( size_t pos, void* buf, size_t size )
{
	assert(size < UINT_MAX);
	if(opened){
		DWORD read_bytes;
		OVERLAPPED Overlapped;
		Overlapped.Offset = pos & 0xffffffff;
#if TRIPLEBIT_32BIT
		Overlapped.OffsetHigh = 0;
#else
		Overlapped.OffsetHigh = pos >> 32;
#endif
		Overlapped.hEvent = NULL;

		assert( size < ULONG_MAX );

		return ReadFile(fd, buf, size, &read_bytes, &Overlapped)
			? (size == read_bytes ? OK : END_OF_FILE) : get_system_error();
	}
	return NOT_OPENED;
}

Status OSFile::read( void* buf, size_t size )
{
	if(opened){
		DWORD read_bytes;

		assert( size < ULONG_MAX );

		return ReadFile(fd, buf, size, &read_bytes, NULL)
			? size == read_bytes ? OK : END_OF_FILE
			: get_system_error();
		return OK;
	}
	return NOT_OPENED;
}

Status OSFile::write( size_t pos, void const* buf, size_t size )
{
	if(opened){
		OVERLAPPED Overlapped;
		DWORD written_bytes;
		Overlapped.Offset = pos & 0xffffffff;
#if TRIPLEBIT_32BIT
		Overlapped.OffsetHigh = 0;
#else
		Overlapped.OffsetHigh = pos >> 32;
#endif
		Overlapped.hEvent = NULL;

		assert ( size < ULONG_MAX );

		return WriteFile(fd, buf, size, &written_bytes, &Overlapped)
			? size == written_bytes ? OK : END_OF_FILE
			: get_system_error();
		return OK;
	}
	return NOT_OPENED;
}

Status OSFile::write( void const* buf, size_t size )
{
	if(opened){
		DWORD written_bytes;

		assert(size < ULONG_MAX );

		return WriteFile(fd,buf, size, &written_bytes, NULL)
			? (DWORD)size == written_bytes ? OK : END_OF_FILE
			: get_system_error();
		return OK;
	}
	return NOT_OPENED;
}

Status OSFile::flush()
{
	if (opened) {
		return FlushFileBuffers(fd) ? OK : get_system_error();
	} else {
		return NOT_OPENED;
	}
}
	

Status OSFile::set_name(char const* new_name)
{
	Status status = close();
	if (status == OK) {
		if (new_name == NULL) {
			name[0] = 0;
		} else {
			if (strcmp(name, new_name) != 0) {

				assert(strlen(new_name) < MAX_PATH_LENGTH);

				if (MoveFile(name, new_name)) {
					strcpy(name,new_name);
				} else {
					return get_system_error();
				}
			}
		}
	}
	return status;
}

Status OSFile::open( AccessMode mode, int flags/*=0*/ )
{
	close();

	assert(name != NULL);

	fd = CreateFile(name,
		win_access_mode[mode],
		FILE_SHARE_READ|FILE_SHARE_WRITE,
		NULL,
		win_open_flags[flags & (FO_TRUNCATE|FO_CREATE)],
		win_open_attrs[(flags & (FO_SYNC|FO_RANDOM)) >> 2],
		NULL);
	this->mode = mode;
	this->flags = flags;
	if (fd == INVALID_HANDLE_VALUE) {
		return get_system_error();
	} else {
		opened = true;
		return OK;
	}
}

Status OSFile::close()
{
	opened = false;
	if(fd==0)
		return OK;
	return CloseHandle(fd) ? OK : get_system_error();
}

Status OSFile::remove()
{
	close();
	return FileDelete(name)
		? OK : get_system_error();
}


Status OSFile::get_size(size_t& size) const
{
	if (opened) {
		LARGE_INTEGER li;
		if(GetFileSizeEx(fd, &li)==0)
			return get_system_error();

		assert( li.QuadPart < SIZE_MAX );

		size = li.QuadPart;

		return OK;
	} else {
		return NOT_OPENED;
	}
}

Status OSFile::set_size(size_t size)
{
	if (opened) {
		LARGE_INTEGER li;
		li.QuadPart = size;
		if (SetFilePointerEx(fd, li, NULL, FILE_BEGIN)){
			if(SetEndOfFile(fd))
				return OK;
		}
		return get_system_error();
	} else {
		return NOT_OPENED;
	}
}
	
bool OSFile::DirectoryExists( const char * path )
{
	DWORD dwAttributes = GetFileAttributes(path);
	if(dwAttributes == INVALID_FILE_ATTRIBUTES )
		return false;
	if(dwAttributes & FILE_ATTRIBUTE_DIRECTORY)
		return true;
	else
		return false;
}

bool OSFile::FileExists( const char * path )
{
	DWORD dwAttributes = GetFileAttributes(path);
	if(dwAttributes == 0xFFFFFFFF)
		return false;
	if(dwAttributes & FILE_ATTRIBUTE_DIRECTORY)
		return false;
	else
		return true;
}

bool OSFile::MkDir( const char * path )
{
	// TODO: log failure
	return (mkdir(path) == 0);
}

bool OSFile::FileDelete( const char * path )
{
	return unlink(path) == 0;
}

size_t OSFile::FileSize(const char * path)
{
	WIN32_FIND_DATA fileInfo;
	size_t ret;
	HANDLE hFind = FindFirstFile(path ,&fileInfo); 
	if(hFind != INVALID_HANDLE_VALUE) 
		 ret = (size_t)fileInfo.nFileSizeLow; 
	FindClose(hFind); 
	return ret;
}

#else
#include <unistd.h>
#include <fcntl.h>
//#include <errno.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/uio.h>

static int unix_access_mode[] = { O_RDONLY, O_WRONLY, O_RDWR };

static int unix_open_flags[] = {
	0, O_TRUNC, O_CREAT, O_CREAT|O_TRUNC,
	O_DSYNC, O_DSYNC|O_TRUNC, O_DSYNC|O_CREAT, O_DSYNC|O_CREAT|O_TRUNC,
};

Status OSFile::set_position(size_t pos)
{
	if (opened) {
		off_t rc, off = off_t(pos);

		rc = lseek(fd, off, SEEK_SET);
		if (rc < 0) {
			return ERR;
		} else if (rc != off) {
			return END_OF_FILE;
		}
		return OK;
	}
	return NOT_OPENED;
}

Status OSFile::get_position(size_t& pos)
{
	if (opened) {
		off_t rc = lseek(fd, 0, SEEK_CUR);
		if (rc < 0) {
			return ERR;
		} else {
			pos = rc;
			return OK;
		}
	}
	return NOT_OPENED;
}

Status OSFile::read(void* buf, size_t size)
{
	if (opened) {
		ssize_t rc = ::read(fd, buf, size);
		if (rc < 0) {
			return ERR;
		} else if (size_t(rc) != size) {
			return END_OF_FILE;
		} else {
			return OK;
		}
	} else {
		return NOT_OPENED;
	}
}

Status OSFile::write(void const* buf, size_t size)
{
	if (opened) {
		ssize_t rc = ::write(fd, buf, size);
		if (rc < 0) {
			return ERR;
		} else if (size_t(rc) != size) {
			return END_OF_FILE;
		} else {return OK;
		}
	} else {
		return NOT_OPENED;
	}
}

Status OSFile::read(size_t pos, void* buf, size_t size)
{
	if (opened) {

		ssize_t rc = pread(fd, buf, size, pos);

		if (rc < 0) {
			return ERR;
		} else if (size_t(rc) != size) {
			return END_OF_FILE;
		} else {
			return OK;
		}
	} else {
		return NOT_OPENED;
	}
}

Status OSFile::write(size_t pos, void const* buf, size_t size)
{
	if (opened) {
		ssize_t rc = pwrite(fd, buf, size, size);

		if (rc < 0) {
			return ERR;
		} else if (size_t(rc) != size) {
			return END_OF_FILE;
		} else {
			return OK;
		}
	} else {
		return NOT_OPENED;
	}
}

Status OSFile::close()
{
	if (opened) {
		opened = false;
		if (::close(fd) != 0) {
			return ERR;
		}
	}
	return OK;
}

Status OSFile::open(AccessMode mode, int flags)
{
	close();

	fd = ::open(name, unix_access_mode[mode] |
		unix_open_flags[flags & (FO_TRUNCATE|FO_SYNC|FO_CREATE)],
		0666);

	opened = false;
	this->mode = mode;
	this->flags = flags;
	if (fd < 0) {
		return ERR;
	} else {
		opened = true;
		return OK;
	}
}

Status OSFile::remove()
{
	close();
	if (unlink(name) < 0) {
		return ERR;
	} else {
		return OK;
	}
}

Status OSFile::get_size(size_t & size) const
{
	if (opened) {
		struct stat fs;
		if (fstat(fd, &fs) == 0) {

			assert( fs.st_size < size_t(-1) );

			size = fs.st_size;
			return OK;
		} else {
			return ERR;
		}
	} else {
		return NOT_OPENED;
	}
}

Status OSFile::set_size(size_t size)
{
	if (opened) {
		if (ftruncate(fd, size) == 0) {
			return OK;
		} else {
			return ERR;
		}
	} else {
		return NOT_OPENED;
	}
}

Status OSFile::flush()
{
	if (opened) {
		if (fsync(fd) != 0) {
			return ERR;
		}
		return OK;
	} else {
		return NOT_OPENED;
	}
}

Status OSFile::set_name(char const* new_name)
{
	Status status = close();
	if (status == OK) {
		if (new_name == NULL) {
			name[0] = 0;
		} else {
			if (strcmp(name, new_name) != 0) {

				assert(  strlen(new_name) < MAX_PATH_LENGTH );

				if (rename(name, new_name)==0) {
					strcpy(name,new_name);
				} else { return ERR;
				}
			}
		}
	}
	return status;
}

void OSFile::get_error_text(Status code, char* buf, size_t buf_size) const
{
	char* msg;
	switch (code) {
	case OK:
		msg = "OK";
		break;
	case NOT_OPENED:
		msg = "file not opened";
		break;
	case END_OF_FILE:
		msg = "operation not completely finished";
		break;
	default:
		msg = strerror(code);
	}
	strncpy(buf, msg, buf_size);
}

bool OSFile::DirectoryExists( const char * path )
{
struct stat sbuff;
	if( stat(path,&sbuff) == 0 ){
		if( S_ISDIR(sbuff.st_mode) )
			return true;
	}
	return false;
}

bool OSFile::FileExists( const char * path )
{
	struct stat sbuff;
	if( stat(path,&sbuff) == 0 ){
		if( S_ISREG(sbuff.st_mode) )
			return true;
	}
	return false;
}

bool OSFile::MkDir( const char * path )
{
	return (::mkdir( path, S_IRWXU|S_IRWXG|S_IRWXO ) == 0);
}

bool OSFile::FileDelete( const char * path )
{
	return unlink(path) == 0;
}

size_t OSFile::FileSize(const char* filename)
{
	struct stat buf;
	stat(filename, &buf);
	return buf.st_size;
}

#endif
