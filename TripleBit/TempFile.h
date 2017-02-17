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

#ifndef _TEMPFILE_H_
#define _TEMPFILE_H_

#include "TripleBit.h"
#include <fstream>
#include <string>
//---------------------------------------------------------------------------
#if defined(_MSC_VER)
typedef unsigned __int64 uint64_t;
#else
#include <stdint.h>
#endif
//---------------------------------------------------------------------------
/// A temporary file
class TempFile {
private:
	/// The next id
	static unsigned id;

	/// The base file name
	std::string baseName;
	/// The file name
	std::string fileName;
	/// The output
	std::ofstream out;

	/// The buffer size
	static const unsigned bufferSize = 16384;
	/// The write buffer
	char writeBuffer[bufferSize];
	/// The write pointer
	unsigned writePointer;

	/// Construct a new suffix
	static std::string newSuffix();
	static std::string newSuffix(unsigned);

public:
	/// Constructor
	TempFile(const std::string& baseName);
	TempFile(const string& baseName,unsigned);
	TempFile(const string baseName,unsigned int,bool flag);
	TempFile(const string& baseName, bool flag1, bool flag2);
    /// Destructor
	~TempFile();

	/// Get the base file name
	const std::string& getBaseFile() const {
		return baseName;
	}
	/// Get the file name
	const std::string& getFile() const {
		return fileName;
	}

	/// Flush the file
	void flush();
	/// Close the file
	void close();
	/// Discard the file
	void discard();

	/// Write a string
	void writeString(unsigned len, const char* str);
	/// Write a id
	/// flag==0 subject
	/// flag==1 object
	/// flag==2 predicate
	void writeId(ID id, unsigned char flag);
	void writeId(ID id);
	/// Raw write
	void write(size_t len, const char* data);

	/// Skip a predicate
	static const char* skipId(const char* reader);
	static const char* skipIdId(const char* reader) {
		return reader + 8;
	}
	/// Skip a string
	static const char* skipString(const char* reader);
	/// Read an id
	static const char* readId(const char* reader, ID& id);
	static int compare21(const char* left, const char* right);
	static int compare12(const char* left, const char* right);
	/// Read a string
	static const char* readString(const char* reader, unsigned& len, const char*& str);

	void writeFloat(float data);
	static const char* readFloat(const char* reader,float& data);

	bool isEmpty(){
		if(writePointer == 0)
			return true;
		else
			return false;
	}
};

//----------------------------------------------------------------------------
/// Maps a file read-only into memory
class MemoryMappedFile
{
   private:
   /// os dependent data
   struct Data;

   /// os dependen tdata
   Data* data;
   /// Begin of the file
   const char* begin;
   /// End of the file
   const char* end;

   public:
   /// Constructor
   MemoryMappedFile();
   /// Destructor
   ~MemoryMappedFile();

   /// Open
   bool open(const char* name);
   /// Close
   void close();

   /// Get the begin
   const char* getBegin() const { return begin; }
   /// Get the end
   const char* getEnd() const { return end; }

   /// Ask the operating system to prefetch a part of the file
   void prefetch(const char* start,const char* end);
};
//---------------------------------------------------------------------------
#endif
