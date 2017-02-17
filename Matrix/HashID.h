//---------------------------------------------------------------------------
// PathGraph
// (c) 2013 Massive Data Management Group @ SCTS & CGCL. 
//     Web site: http://grid.hust.edu.cn/triplebit
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------

#ifndef HASHID_H_
#define HASHID_H_

#include "MemoryBuffer.h"
#include <vector>
#include "MMapBuffer.h"
#include "TempFile.h"
#include "MutexLock.h"
#include "EntityIDBuffer.h"
#include <Matrixmap.h>

#define MEMCPY_SIZE (16 * 1024 * 1024)

struct Degree{
	unsigned indeg;
	unsigned outdeg;
};

class HashID {
private:
	ID *oldID_mapto_newID;
	ID *newID_mapto_oldID;

	ID maxID;
	ID maxFromID;
	unsigned maxCount;
	size_t totalCouple;
	unsigned adjNum;

	vector<char *> adj;
	unsigned *global_adj_index;

	vector<MMapBuffer *> adjMap;
	MMapBuffer *global_adj_indexMap;

	ID start_ID[100];
	Degree* degree;

	stack<ID> s;
	ID idcount;
	static string Dir;

public:
	HashID(ID maxID);
	HashID(string dir);
	virtual ~HashID();
	void init(bool forward_or_backward = true);

	status convert_edge(string inputfile, unsigned vertexNum);
	status convert_adj(string inputfile, unsigned vertexNum);
	status convert_adj_nonum(string inputfile, unsigned vertexNum);
	status convert_adj_to_reverse_adj(string inputfile, unsigned vertexNum);

	void sort_degree(unsigned fileindex);
	void DFS(bool forward_or_backward = true);
	ID convertToRaw(TempFile *rawFile, bool forward_or_backward = true);

	unsigned getAdjNum(){ return adjNum; }
	unsigned getMaxID(){ return maxID; }
	static void FIXLINE(char * s);

	static size_t loadFileinMemory(const char* filePath, char*& buf);
	static void parallel_load_task(char *&buf, MemoryMappedFile *&temp, unsigned long cur_pos, size_t copy_size);
	static void parallel_load_inmemory(const char* filePath, char*& buf);

	void encode(bool forward_or_backward);

private:
	inline bool isNewIDExist(ID id, ID& newoff);
	inline bool setNew(ID pos, ID oldID);
	inline bool setOld(ID pos, ID newID);
	char* getOffset(ID);

	inline bool zeroDegree(ID id);
	inline bool both_in_and_out_vertex(ID id);
	void get_root_vertices(ID maxID);

	unsigned DFS_V(ID);
	unsigned DFS_V(ID v, bool* &neighbors_visited, ID &newRootID);

	unsigned bfs_tree_builder(ID rootID, bool *&visited, hash_map<ID, size_t> &id_offset);
	void dfs_forward_encode(ID rootID, bool *&visited, hash_map<ID, size_t> id_offset, ID &newRootID);
	void dfs_backward_encode(ID rootID, bool *&visited, hash_map<ID, size_t> id_offset, ID &newRootID, unsigned unique_vertices_num);
	void save_encoded_ids(bool forward_or_backward);
};
#endif /* HASHID_H_ */
