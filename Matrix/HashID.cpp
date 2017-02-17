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

#include "HashID.h"
#include "TempFile.h"
#include "OSFile.h"
#include "Sorter.h"
#include <list>

string HashID::Dir = "";
Degree *globalDegree;

HashID::HashID(unsigned maxID) {
	// TODO Auto-generated constructor stub
}

HashID::HashID(string dir) {
	Dir = dir;
	oldID_mapto_newID = NULL;
	newID_mapto_oldID = NULL;
	degree = NULL;
	idcount = 0;
	maxID = 0;
	maxFromID = 0;
}

HashID::~HashID() {
	// TODO Auto-generated destructor stub
	unsigned i = 0;
	unsigned adjSize = adjMap.size();

	if (oldID_mapto_newID){
		free(oldID_mapto_newID);
		oldID_mapto_newID = NULL;
	}

	if (newID_mapto_oldID) {
		free(newID_mapto_oldID);
		newID_mapto_oldID = NULL;
	}

	for (i = 0; i < adjMap.size(); i++) {
		delete adjMap[i];
		adjMap[i] = NULL;
		adj[i] = NULL;
	}
	adjMap.clear();
	adj.clear();

	if(global_adj_indexMap){
		delete global_adj_indexMap;
		global_adj_indexMap = NULL;
		global_adj_index = NULL;
	}

	if (degree) {
		free(degree);
		degree = NULL;
	}

	char fileName[256];
	for (i = 0; i < adjSize; i++) {
		sprintf(fileName, "%s/adj.%d", Dir.c_str(), i);
		OSFile::FileDelete(fileName);
	}
	sprintf(fileName, "%s/global_adj_index.0", Dir.c_str());
	OSFile::FileDelete(fileName);
}

void HashID::FIXLINE(char * s) {
	int len = (int) strlen(s) - 1;
	if (s[len] == '\n')
		s[len] = 0;
}

bool HashID::isNewIDExist(ID id, ID& newoff) {
	ID temp = *(oldID_mapto_newID + id);
	if (temp) {
		newoff = temp;
		return true;
	} else {
		return false;
	}
}

bool HashID::setNew(ID pos, ID oldID) {
	*(newID_mapto_oldID +pos) = oldID;
	 return true;
}

bool HashID::setOld(ID pos, ID newID) {
	*(oldID_mapto_newID + pos) = newID;
	return true;
}

void HashID::init(bool forward_or_backward) {
	unsigned fileindex = 0;
	char adjPath[150];
	char adj_indexPath[150];
	sprintf(adjPath, "%s/adj.%d", Dir.c_str(), fileindex);

	while (OSFile::FileExists(adjPath) == true) {
		adjMap.push_back(new MMapBuffer(adjPath, 0));
		adj.push_back(adjMap[fileindex]->get_address());
		fileindex++;
		sprintf(adjPath, "%s/adj.%d", Dir.c_str(), fileindex);
	}

	sprintf(adj_indexPath, "%s/global_adj_index.0", Dir.c_str());
	global_adj_indexMap = new MMapBuffer(adj_indexPath, 0);
	global_adj_index = (unsigned *)global_adj_indexMap->get_address();
}

status HashID::convert_edge(string inputfile, unsigned vertexNum) {
	maxID = 0;
	maxFromID = 0;
	maxCount = 0;
	totalCouple = 0;
	vertexNum += 1;
	FILE * inf = fopen(inputfile.c_str(), "r");
	size_t bytesread = 0;
	size_t linenum = 0;

	if (inf == NULL) {
		cout << "Could not load :" << inputfile << " error: " << strerror(errno)
				<< std::endl;
	}
	assert(inf != NULL);

	TempFile* tempFile = new TempFile("edge_format");
	cout << "Reading in edge list format!" << std::endl;
	char s[1024];
	while (fgets(s, 1024, inf) != NULL) {
		linenum++;
		if (linenum % 10000000 == 0) {
			cout << "Read " << linenum << " lines, " << bytesread / 1024 / 1024.
					<< " MB" << std::endl;
		}
		FIXLINE(s);
		bytesread += strlen(s);
		if (s[0] == '#')
			continue; // Comment
		if (s[0] == '%')
			continue; // Comment

		char delims[] = "\t ";
		char * t;
		t = strtok(s, delims);
		if (t == NULL) {
			cout << "Input file is not in right format. "
					<< "Expecting \"<from>\t<to>\". " << "Current line: \"" << s
					<< "\"\n";
			assert(false);
		}
		ID from = atoi(t);
		t = strtok(NULL, delims);
		if (t == NULL) {
			cout << "Input file is not in right format. "
					<< "Expecting \"<from>\t<to>\". " << "Current line: \"" << s
					<< "\"\n";
			assert(false);
		}
		ID to = atoi(t);
		tempFile->writeId(from);
		tempFile->writeId(to);
	}
	fclose(inf);
	tempFile->flush();

	TempFile* sortFile = new TempFile("sortFile");
	Sorter::sort(*tempFile, *sortFile, TempFile::skipIdId, TempFile::compare12);
	tempFile->discard();
	delete tempFile;

	MemoryMappedFile mappedIn;
	assert(mappedIn.open(sortFile->getFile().c_str()));
	const char* reader = mappedIn.getBegin(), *begin = reader, *limit = mappedIn.getEnd();

	bool firstInsert = true, firstTime = true;
	int fileindex = -1;
	unsigned fileOff = 0;

	vector<TempFile *> adjFile;
	size_t pageSize = vertexNum * sizeof(ID) / (MemoryBuffer::pagesize) + (((vertexNum * sizeof(ID)) % (MemoryBuffer::pagesize)) ? 1 : 0);
	char adj_indexPath[150];
	sprintf(adj_indexPath, "%s/global_adj_index.0", Dir.c_str());
	MMapBuffer *global_adj_indexFile = new MMapBuffer(adj_indexPath, 2 * pageSize * MemoryBuffer::pagesize);
	unsigned *global_adj_index = (unsigned *)global_adj_indexFile->get_address();

	unsigned from = 0, to = 0, lastfrom = *(ID*) reader;
	EntityIDBuffer* tempEnt = new EntityIDBuffer();

	while (reader < limit) {
		from = *(ID*) reader;
		reader += sizeof(ID);
		to = *(ID*) reader;
		reader += sizeof(ID);
		maxID = std::max(from, maxID);
		maxID = std::max(to, maxID);
		maxFromID = std::max(from, maxFromID);

		if (firstInsert) {
			tempEnt->insertID(from);
			tempEnt->insertID(0); // for num
			tempEnt->insertID(to);
			firstInsert = false;
		} else if (from != lastfrom) {
			unsigned num = tempEnt->getSize() - 2;
			tempEnt->getBuffer()[1] = num; //update num
			unsigned size = tempEnt->getSize() * sizeof(ID);
			if (!firstTime && fileOff + size < (unsigned) (-1) / 8) {
				adjFile[fileindex]->write(size, (const char*) tempEnt->getBuffer());
				global_adj_index[2 * lastfrom] = fileOff;
				global_adj_index[2 * lastfrom + 1] = fileindex;
				fileOff += size;
			} else {
				if (fileindex >= 0) {
					adjFile[fileindex]->close();
				}

				fileindex++;
				fileOff = 0;
				adjFile.push_back(new TempFile(Dir + "/adj", fileindex));
				start_ID[fileindex] = lastfrom;
				adjFile[fileindex]->write(size,(const char*) tempEnt->getBuffer());
				global_adj_index[2 * lastfrom] = 1;//actually is '0', but we set '1' here, for '0' has been used to mark the unexist 'from data'
				global_adj_index[2 * lastfrom + 1] = fileindex;
				fileOff += size;
				firstTime = false;

				cout << "fileIndex:" << fileindex << ", from minID " << from << " to maxID " << maxID << endl;
			}
			maxCount = max(maxCount, num);

			tempEnt->empty();
			tempEnt->insertID(from);
			tempEnt->insertID(0);
			tempEnt->insertID(to);
			lastfrom = from;
		} else {
			tempEnt->insertID(to);
		}
	}
	adjFile[fileindex]->close();
	global_adj_indexFile->flush();

	adjNum = adjFile.size();
	for(int i = 0;i < adjFile.size();i++){
		delete adjFile[i];
		adjFile[i] = NULL;
	}
	adjFile.clear();
	delete global_adj_indexFile;

	mappedIn.close();
	sortFile->discard();
	delete tempEnt;
	delete sortFile;

	return OK;
}

status HashID::convert_adj(string inputfile, unsigned vertexNum) {
	maxID = 0;
	maxFromID = 0;
	maxCount = 0;
	totalCouple = 0;
	vertexNum += 1;
	degree = (Degree*) calloc(vertexNum, sizeof(Degree));
	assert(degree);
	globalDegree = degree;

	FILE * inf = fopen(inputfile.c_str(), "r");
	if (inf == NULL) {
		cerr << "Could not load :" << inputfile << " error: " << strerror(errno)
				<< std::endl;
	}
	assert(inf != NULL);
	cout << "Reading in adjacency list format!" << inputfile << std::endl;

	int maxlen = 100000000;
	char * s = (char*) malloc(maxlen);

	size_t bytesread = 0;

	char delims[] = " \t";
	size_t linenum = 0;
	size_t lastlog = 0;

	unsigned from = 0, to = 0;
	EntityIDBuffer* tempEnt = new EntityIDBuffer();

	/* PHASE 1 - count */
	bool firstTime = true;
	int fileindex = -1;
	unsigned fileOff = 0;
	vector<TempFile*> adjFile;
	size_t pageSize = vertexNum * sizeof(ID) / (MemoryBuffer::pagesize) + (((vertexNum * sizeof(ID)) % (MemoryBuffer::pagesize)) ? 1 : 0);
	char adj_indexPath[150];
	sprintf(adj_indexPath, "%s/global_adj_index.0", Dir.c_str());
	MMapBuffer *global_adj_indexFile = new MMapBuffer(adj_indexPath, 2 * pageSize * MemoryBuffer::pagesize);
	unsigned *global_adj_index = (unsigned *)global_adj_indexFile->get_address();

	while (fgets(s, maxlen, inf) != NULL) {
		linenum++;
		if (bytesread - lastlog >= 500000000) {
			cerr << "Read " << linenum << " lines, " << bytesread / 1024 / 1024.
					<< " MB" << std::endl;
			lastlog = bytesread;
		}
		FIXLINE(s);
		bytesread += strlen(s);

		if (s[0] == '#')
			continue; // Comment
		if (s[0] == '%')
			continue; // Comment
		char *t = strtok(s, delims);
		from = atoi(t);

		maxID = std::max(from, maxID);
		maxFromID = std::max(from, maxFromID);

		tempEnt->empty();
		tempEnt->insertID(from);

		t = strtok(NULL, delims);
		if (t != NULL) {
			unsigned num = atoi(t);
			unsigned i = 0;
			if (num == 0)
				continue;
			tempEnt->insertID(num);

			while ((t = strtok(NULL, delims)) != NULL) {
				to = atoi(t);

				if (from != to) {
					maxID = std::max(to, maxID);
					degree[from].outdeg++;
					degree[to].indeg++;
					tempEnt->insertID(to);
				}
				i++;
			}

			unsigned size = tempEnt->getSize() * sizeof(ID);
			if (!firstTime && fileOff + size < (unsigned) (-1) / 8) {
				adjFile[fileindex]->write(size,(const char*) tempEnt->getBuffer());
				global_adj_index[2 * from] = fileOff;
				global_adj_index[2 * from + 1] = fileindex;
				fileOff += size;
			} else {
				if (fileindex >= 0) {
					adjFile[fileindex]->close();
				}
				fileindex++;
				fileOff = 0;
				adjFile.push_back(new TempFile(Dir + "/adj", fileindex));
				start_ID[fileindex] = from;
				adjFile[fileindex]->write(size,(const char*) tempEnt->getBuffer());
				global_adj_index[2 * from] = 1;//actually is '0', but we set '1' here, for '0' has been used to mark the unexist 'from data'
				global_adj_index[2 * from + 1] = fileindex;
				fileOff += size;
				firstTime = false;

				cout << "fileIndex:" << fileindex << "  from minID " << from
						<< " to maxID " << maxID << endl;
			}
			maxCount = max(maxCount, i);
			if (num != i)
				cerr << "Mismatch when reading adjacency list: " << num
						<< " != " << i << " s: " << std::string(s)
						<< " on line: " << linenum << std::endl;
			assert(num == i);
			totalCouple += num;
		}

	}
	adjFile[fileindex]->close();
	global_adj_indexFile->flush();

	adjNum = adjFile.size();
	for(int i = 0;i < adjFile.size();i++){
		delete adjFile[i];
		adjFile[i] = NULL;
	}
	adjFile.clear();
	delete global_adj_indexFile;

	cout << "finish encode...  maxID:" << maxID << endl;
	delete tempEnt;
	free(s);
	fclose(inf);
	return OK;
}

status HashID::convert_adj_nonum(string inputfile, unsigned vertexNum) {
	maxID = 0;
	maxFromID = 0;
	maxCount = 0;
	totalCouple = 0;
	vertexNum += 1;
	degree = (Degree*) calloc(vertexNum, sizeof(Degree));
	assert(degree);
	globalDegree = degree;

	FILE * inf = fopen(inputfile.c_str(), "r");
	if (inf == NULL) {
		cerr << "Could not load :" << inputfile << " error: " << strerror(errno)
				<< std::endl;
	}
	assert(inf != NULL);
	cout << "Reading in adjacency list format!" << std::endl;

	int maxlen = 100000000;
	char * s = (char*) malloc(maxlen);

	size_t bytesread = 0;

	char delims[] = " \t";
	size_t linenum = 0;
	size_t lastlog = 0;

	unsigned from = 0, to = 0;
	EntityIDBuffer* tempEnt = new EntityIDBuffer();

	/* PHASE 1 - count */
	bool firstTime = true;
	int fileindex = -1;
	unsigned fileOff = 0;
	vector<TempFile*> adjFile;
	size_t pageSize = vertexNum * sizeof(ID) / (MemoryBuffer::pagesize) + (((vertexNum * sizeof(ID)) % (MemoryBuffer::pagesize)) ? 1 : 0);
	char adj_indexPath[150];
	sprintf(adj_indexPath, "%s/global_adj_index.0", Dir.c_str());
	MMapBuffer *global_adj_indexFile = new MMapBuffer(adj_indexPath, 2 * pageSize * MemoryBuffer::pagesize);
	unsigned *global_adj_index = (unsigned *)global_adj_indexFile->get_address();

	while (fgets(s, maxlen, inf) != NULL) {
		linenum++;
		if (bytesread - lastlog >= 500000000) {
			cerr << "Read " << linenum << " lines, " << bytesread / 1024 / 1024.
					<< " MB" << std::endl;
			lastlog = bytesread;
		}
		FIXLINE(s);
		bytesread += strlen(s);

		if (s[0] == '#')
			continue; // Comment
		if (s[0] == '%')
			continue; // Comment
		char * t = strtok(s, delims);
		from = atoi(t);

		maxID = std::max(from, maxID);
		maxFromID = std::max(from, maxFromID);

		tempEnt->empty();
		tempEnt->insertID(from);
		unsigned num = 0;
		tempEnt->insertID(num);

		while ((t = strtok(NULL, delims)) != NULL) {
			to = atoi(t);
			if (from != to) {
				maxID = std::max(to, maxID);
				degree[from].outdeg++;
				degree[to].indeg++;
				tempEnt->insertID(to);
				num++;
			}
		}

		tempEnt->getBuffer()[1] = num;
		totalCouple += num;
		unsigned size = tempEnt->getSize() * sizeof(ID);
		if (!firstTime && fileOff + size < (unsigned) (-1) / 8) {
			adjFile[fileindex]->write(size, (const char*) tempEnt->getBuffer());
			global_adj_index[2 * from] = fileOff;
			global_adj_index[2 * from + 1] = fileindex;
			fileOff += size;
		} else {
			if (fileindex >= 0) {
				adjFile[fileindex]->close();
			}

			fileindex++;
			fileOff = 0;
			adjFile.push_back(new TempFile(Dir + "/adj", fileindex));
			start_ID[fileindex] = from;
			adjFile[fileindex]->write(size, (const char*) tempEnt->getBuffer());
			global_adj_index[2 * from] = 1;//actually is '0', but we set '1' here, for '0' has been used to mark the unexist 'from data'
			global_adj_index[2 * from + 1] = fileindex;
			fileOff += size;
			firstTime = false;

			cout << "fileIndex: " << fileindex << ", from minID " << from << " to maxID "<< maxID << endl;
		}
		maxCount = max(maxCount, num);

	}
	adjFile[fileindex]->close();
	global_adj_indexFile->flush();

	adjNum = adjFile.size();
	for(int i = 0;i < adjFile.size();i++){
		delete adjFile[i];
		adjFile[i] = NULL;
	}
	adjFile.clear();
	delete global_adj_indexFile;

	cout << "finish encode...  maxID:" << maxID << endl;
	delete tempEnt;
	free(s);
	fclose(inf);
	return OK;
}

status HashID::convert_adj_to_reverse_adj(string inputfile, unsigned vertexNum) {
	maxID = 0;
	maxFromID = 0;
	maxCount = 0;
	totalCouple = 0;
	vertexNum += 1;
	degree = (Degree*) calloc(vertexNum, sizeof(Degree));
	assert(degree);
	globalDegree = degree;

	FILE * inf = fopen(inputfile.c_str(), "r");
	if (inf == NULL) {
		cerr << "Could not load :" << inputfile << ", error: "
				<< strerror(errno) << std::endl;
	}
	assert(inf != NULL);
	cout << "Reading in adjacency list format!" << std::endl;

	int maxlen = 100000000;
	char *s = (char*) malloc(maxlen);
	size_t bytesread = 0, lastlog = 0;
	char delims[] = " \t";
	size_t linenum = 0;
	unsigned from = 0, to = 0;
	TempFile* tempFile = new TempFile("edge_format");

	while (fgets(s, maxlen, inf) != NULL) {
		linenum++;
		if (bytesread - lastlog >= 500000000) {
			cerr << "Read " << linenum << " lines, " << bytesread / 1024 / 1024.
					<< " MB" << std::endl;
			lastlog = bytesread;
		}
		FIXLINE(s);
		bytesread += strlen(s);

		if (s[0] == '#')
			continue; // Comment
		if (s[0] == '%')
			continue; // Comment
		char *t = strtok(s, delims);
		from = atoi(t);

		while ((t = strtok(NULL, delims)) != NULL) {
			to = atoi(t);
			tempFile->writeId(from);
			tempFile->writeId(to);
			if (from != to) {
				degree[from].outdeg++;
				degree[to].indeg++;
			}
			totalCouple++;
		}
	}
	tempFile->flush();
	fclose(inf);
	free(s);

	TempFile* sortFile = new TempFile("sortFile");
	Sorter::sort(*tempFile, *sortFile, TempFile::skipIdId, TempFile::compare21);
	tempFile->discard();
	delete tempFile;
	cout<<"success convert into edge format"<<endl;

	MemoryMappedFile mappedIn;
	assert(mappedIn.open(sortFile->getFile().c_str()));
	const char* reader = mappedIn.getBegin(), *begin = reader, *limit = mappedIn.getEnd();

	bool firstInsert = true, firstTime = true;
	int fileindex = -1;
	unsigned fileOff = 0;
	unsigned lastto = *(ID *)(reader + sizeof(ID));
	EntityIDBuffer* tempEnt = new EntityIDBuffer();
	vector<TempFile*> adjFile;
	size_t pageSize = vertexNum * sizeof(ID) / (MemoryBuffer::pagesize) + (((vertexNum * sizeof(ID)) % (MemoryBuffer::pagesize)) ? 1 : 0);
	char adj_indexPath[150];
	sprintf(adj_indexPath, "%s/global_adj_index.0", Dir.c_str());
	MMapBuffer *global_adj_indexFile = new MMapBuffer(adj_indexPath, 2 * pageSize * MemoryBuffer::pagesize);
	unsigned *global_adj_index = (unsigned *)global_adj_indexFile->get_address();

	while (reader < limit) {
		from = *(ID*) reader;
		reader += sizeof(ID);
		to = *(ID*) reader;
		reader += sizeof(ID);

		maxID = std::max(from, maxID);
		maxID = std::max(to, maxID);
		maxFromID = std::max(to, maxFromID);

		if (firstInsert) {
			tempEnt->insertID(to);
			tempEnt->insertID(0); // for num
			tempEnt->insertID(from);
			firstInsert = false;
		} else if (to != lastto) {
			unsigned num = tempEnt->getSize() - 2;
			tempEnt->getBuffer()[1] = num; //update num
			unsigned size = tempEnt->getSize() * sizeof(ID);
			if (!firstTime && fileOff + size < (unsigned) (-1) / 8) {
				adjFile[fileindex]->write(size,(const char*) tempEnt->getBuffer());
				global_adj_index[2 * lastto] = fileOff;
				global_adj_index[2 * lastto + 1] = fileindex;
				fileOff += size;
			} else {
				if (fileindex >= 0) {
					adjFile[fileindex]->close();
				}

				fileindex++;
				fileOff = 0;
				adjFile.push_back(new TempFile(Dir + "/adj", fileindex));
				start_ID[fileindex] = lastto;
				adjFile[fileindex]->write(size,(const char*) tempEnt->getBuffer());
				global_adj_index[2 * lastto] = 1;//actually is '0', but we set '1' here, for '0' has been used to mark the unexist 'to data'
				global_adj_index[2 * lastto + 1] = fileindex;
				fileOff += size;
				firstTime = false;

				cout << "fileIndex: " << fileindex << ", from minID " << to<< " to maxID " << maxID << endl;
			}
			maxCount = max(maxCount, num);

			tempEnt->empty();
			tempEnt->insertID(to);
			tempEnt->insertID(0);
			tempEnt->insertID(from);
			lastto = to;
		} else {
			tempEnt->insertID(from);
		}
	}
	adjFile[fileindex]->close();
	global_adj_indexFile->flush();
	cout<<"success convert into reverse adjacency list"<<endl;

	adjNum = adjFile.size();
	for(int i = 0;i < adjFile.size();i++){
		delete adjFile[i];
		adjFile[i] = NULL;
	}
	adjFile.clear();
	delete global_adj_indexFile;

	mappedIn.close();
	sortFile->discard();
	delete sortFile;
	delete tempEnt;

	return OK;
}

char* HashID::getOffset(ID id) {
	if (id > maxFromID || id < start_ID[0])return NULL;

	unsigned fileindex = global_adj_index[2 * id + 1];
	unsigned adjOff = global_adj_index[2 * id];
	if(adjOff == 0)return NULL; //'0' indicate unexist 'from or to data'
	if(adjOff == 1)adjOff = 0; //we modify the '1' back to '0' here
	char *temp = (adj[fileindex] + adjOff);
	return temp;
}

int qcomparedegree(const void* a, const void* b) {
	ID p1 = *(const ID*) a;
	ID p2 = *(const ID*) b;
	p1 = globalDegree[p1].outdeg - globalDegree[p1].indeg;
	p2 = globalDegree[p2].outdeg - globalDegree[p2].indeg;
	return p1 - p2;
}

int qcompareoutdegree(const void* a, const void* b) {
	//sort from large to small
	ID p1 = *(const ID*) a;
	ID p2 = *(const ID*) b;
	p1 = globalDegree[p1].outdeg;
	p2 = globalDegree[p2].outdeg;
	return p2 - p1;
}

int qcompareindegree(const void* a, const void* b) {
	//sort from small to large
	ID p1 = *(const ID*) a;
	ID p2 = *(const ID*) b;
	p1 = globalDegree[p1].indeg;
	p2 = globalDegree[p2].indeg;
	return p1 - p2;
}

int qcompareindegree21(const void* a, const void* b) {
	//sort from large to small
	ID p1 = *(const ID*) a;
	ID p2 = *(const ID*) b;
	p1 = globalDegree[p1].indeg;
	p2 = globalDegree[p2].indeg;
	return p2 - p1;
}

bool HashID::zeroDegree(ID id) {
	if (!degree[id].indeg && !degree[id].outdeg)
		return true;
	else
		return false;
}

bool HashID::both_in_and_out_vertex(ID id) {
	if (degree[id].indeg && degree[id].outdeg)
		return true;
	else
		return false;
}

void HashID::get_root_vertices(ID maxID) {
	EntityIDBuffer* buffer = new EntityIDBuffer();
	buffer->setIDCount(1);

	for (ID start = 0; start <= maxID; start++) {
		if (!degree[start].indeg && degree[start].outdeg) {
			buffer->insertID(start);
		}
	}
	qsort((void*) buffer->getBuffer(), buffer->getSize(), 4, qcompareoutdegree);
	if (buffer->getSize() == 0)buffer->insertID(0);
	cout << "forward root number: " << buffer->getSize() << endl;
	TempFile* rootFile = new TempFile(Dir + "/root.forward", 0);
	rootFile->write((size_t) buffer->getSize() * sizeof(ID),(const char*) buffer->getBuffer());
	rootFile->flush();
	delete rootFile;

	buffer->empty();
	for (ID start = 0; start <= maxID; start++) {
		if (degree[start].indeg && !degree[start].outdeg) {
		//if ((degree[start].indeg > 1) && !degree[start].outdeg) {
			buffer->insertID(start);
		}
	}
	qsort((void*) buffer->getBuffer(), buffer->getSize(), 4, qcompareindegree21);
	if (buffer->getSize() == 0)buffer->insertID(0);
	cout << "backward root number: " << buffer->getSize() << endl;
	rootFile = new TempFile(Dir + "/root.backward", 0);
	rootFile->write((size_t) buffer->getSize() * sizeof(ID),(const char*) buffer->getBuffer());
	rootFile->flush();
	delete rootFile;

	cout << "finish writing rootFile" << endl;
	delete buffer;
}

void HashID::sort_degree(unsigned fileindex) {
	cout << "sort:" << fileindex << "  begin..." << endl;
	if (fileindex == adj.size()) {
		get_root_vertices(maxID);
		return;
	}

	size_t whenflush = 0;
	char* begin = adjMap[fileindex]->get_address();
	char* limit = begin + adjMap[fileindex]->get_length();
	char* reader = begin;
	size_t count = 0;
	size_t mcount = maxCount;
	ID* sortBuf = (ID*) malloc(mcount * 4);
	while (reader < limit) {
		reader += 4; //skip from ID;
		count = *(ID*) reader;
		reader += 4; //skip count;
		if (count > mcount) {
			mcount = count;
			sortBuf = (ID*) realloc(sortBuf, mcount * sizeof(ID));
		}
		memcpy(sortBuf, reader, count * sizeof(ID));
		//qsort((void*)sortBuf,count,sizeof(ID),qcomparedegree);
		qsort((void*) sortBuf, count, sizeof(ID), qcompareindegree);
		memcpy(reader, sortBuf, count * sizeof(ID));
		reader += count * 4; //skip to ID;

		whenflush += (8 + count * 4);

		if (whenflush > (2 << 27)) {
			adjMap[fileindex]->flush();
			whenflush = 0;
		}
	}

	adjMap[fileindex]->flush();
	free(sortBuf);
	cout << "sort:" << fileindex << " end..." << endl;
}

size_t HashID::loadFileinMemory(const char* filePath, char*& buf) {
	MemoryMappedFile temp;
	temp.open(filePath);
	size_t size = temp.getEnd() - temp.getBegin();
	assert(size);
	buf = (char*) malloc(size);
	assert(buf);
	memcpy(buf, temp.getBegin(), size);
	temp.close();
	return size;
}

void HashID::parallel_load_task(char *&buf, MemoryMappedFile *&temp,
		unsigned long cur_pos, size_t copy_size) {
	memcpy(buf + cur_pos, temp->getBegin() + cur_pos, copy_size);
}

void HashID::parallel_load_inmemory(const char* filePath, char*& buf) {
	MemoryMappedFile *temp = new MemoryMappedFile();
	temp->open(filePath);
	size_t size = temp->getEnd() - temp->getBegin();
	assert(size);
	buf = (char*) malloc(size);
	assert(buf);
	size_t task_num = size / MEMCPY_SIZE, left = size % MEMCPY_SIZE;
	unsigned long cur_pos = 0;

	for (size_t i = 0; i < task_num; i++) {
		CThreadPool::getInstance().AddTask(
				boost::bind(&HashID::parallel_load_task, buf, temp, cur_pos, MEMCPY_SIZE));
		cur_pos += MEMCPY_SIZE;
	}
	if (left)
		CThreadPool::getInstance().AddTask(
				boost::bind(&HashID::parallel_load_task, buf, temp, cur_pos, left));
	CThreadPool::getInstance().Wait();

	temp->close();
	delete temp;
}

unsigned HashID::DFS_V(ID v) {
	s.push(v);
	unsigned off = 0;
	ID* addr = NULL;
	unsigned count = 0;

	while (!s.empty()) {
		int w = s.top();
		s.pop();
		if (isNewIDExist(w, off) == false) {
			setOld(w, idcount);
			setNew(idcount, w);
			idcount++;

			addr = (ID*) getOffset(w);
			if (addr == NULL)
				continue;
			assert(w == *(addr));
			count = *(addr + 1);
			addr += 2; //skip the id and count

			for (int i = 0; i < count; i++) {
				ID temp = *(addr + i);
				if (isNewIDExist(temp, off) == false) {
					s.push(temp);
				}
			}
		}
	}

	return idcount;
}

unsigned HashID::DFS_V(ID v, bool* &neighbors_visited, ID &newRootID) {
	s.push(v);
	unsigned off = 0;
	ID* addr = NULL;
	unsigned count = 0;

	while (!s.empty()) {
		int w = s.top();
		s.pop();
		if (isNewIDExist(w, off) == false) {
			setOld(w, idcount);
			setNew(idcount, w);
			idcount++;

			addr = (ID*) getOffset(w);
			if (addr == NULL)
				continue;
			assert(w == *(addr));
			count = *(addr + 1);
			addr += 2; //skip the id and count

			for (int i = 0; i < count; i++) {
				ID temp = *(addr + i);
				if (isNewIDExist(temp, off) == false) {
					setOld(temp, idcount);
					setNew(idcount, temp);
					idcount++;
					s.push(temp);
				}
			}
			neighbors_visited[w] = true;
		} else if (!neighbors_visited[w]) {
			addr = (ID*) getOffset(w);
			if (addr == NULL)
				continue;
			assert(w == *(addr));
			count = *(addr + 1);
			addr += 2; //skip the id and count

			for (int i = 0; i < count; i++) {
				ID temp = *(addr + i);
				if (isNewIDExist(temp, off) == false) {
					setOld(temp, idcount);
					setNew(idcount, temp);
					idcount++;
					s.push(temp);
				}
			}
			neighbors_visited[w] = true;
		}
	}

	newRootID = oldID_mapto_newID[v];
	return idcount;
}

void HashID::DFS(bool forward_or_backward) {
	cout << "in DFS Encode, maxID: " << maxID << endl;
	unsigned leftLimit = 10;
	idcount = 1;
	newID_mapto_oldID = (ID *)calloc(maxID + 1, sizeof(ID));
	oldID_mapto_newID = (ID *)calloc(maxID + 1, sizeof(ID));

	MemoryMappedFile *rootMap = new MemoryMappedFile();
	if(forward_or_backward == true)assert(rootMap->open((Dir+"/root.forward.0").c_str()));
	else assert(rootMap->open((Dir+"/root.backward.0").c_str()));
	unsigned rootNum = (rootMap->getEnd() - rootMap->getBegin()) / sizeof(ID);
	ID *root = (ID*) rootMap->getBegin();
	if (rootNum > 0)cout << "root:" << *(root) << "  rootNum:" << rootNum << endl;

	unsigned newNum = 0;
	unsigned i = 0;
	bool *neighbor_visited = (bool *) calloc((maxID + 1), sizeof(bool));

	ID newRootID = 0;
	TempFile *newRoot;
	if(forward_or_backward == true)newRoot= new TempFile(Dir + "/new_root.forward", 0);
	else newRoot= new TempFile(Dir + "/new_root.backward", 0);
	for (i = 0; i < rootNum; i++) {
		newNum = DFS_V(*(root + i), neighbor_visited, newRootID);
		newRoot->writeId(newRootID);
		if (newNum >= maxID - leftLimit)
			break;
	}
	cout << "root compute end" << endl;
	unsigned off = 0;

	if (newNum < maxID - leftLimit) {
		for (i = start_ID[0]; i <= maxID; i++) {
			if (zeroDegree(i) == false && isNewIDExist(i, off) == false) {
				newNum = DFS_V(i, neighbor_visited, newRootID);
				newRoot->writeId(newRootID);
				if (newNum >= maxID - leftLimit)
					break;
			}
		}

	}
	free(neighbor_visited);

	if (newNum >= maxID - leftLimit) {
		for (i = start_ID[0]; i <= maxID; i++) {
			if (zeroDegree(i) == false && isNewIDExist(i, off) == false) {
				setOld(i, newNum);
				setNew(newNum, i);
				newRoot->writeId(newNum);
				newNum++;
			}
		}
		cout << "newNum:" << newNum << "  maxID:" << maxID << endl;
	}

	newRoot->flush();
	rootMap->close();
	delete newRoot;
	delete rootMap;
	remove((Dir+"/root.forward.0").c_str());
	remove((Dir+"/root.backward.0").c_str());
	rootMap = NULL;
	maxID = newNum - 1;

	TempFile *old_mapto_new = new TempFile(Dir + "/old_mapto_new", 0);
	old_mapto_new->write((maxID + 1) * sizeof(ID), (const char*) oldID_mapto_newID);
	old_mapto_new->close();
	delete old_mapto_new;
	old_mapto_new = NULL;

	TempFile *new_mapto_old = new TempFile(Dir + "/new_mapto_old", 0);
	new_mapto_old->write((maxID + 1) * sizeof(ID), (const char*) newID_mapto_oldID);
	new_mapto_old->close();
	delete new_mapto_old;
	new_mapto_old = NULL;

	free(newID_mapto_oldID);
	newID_mapto_oldID = NULL;
	cout << "end write file old_mapto_new and new_mapto_old, size: " << (maxID + 1) * sizeof(ID) << endl;
}

ID HashID::convertToRaw(TempFile* rawFile, bool forward_or_backward) {
	if (degree) {
		free(degree);
		degree = NULL;
	}
	degree = (Degree*) calloc(maxID + 1, sizeof(Degree));

	for (unsigned fileindex = 0; fileindex < adjMap.size(); fileindex++) {
		char* begin = adjMap[fileindex]->get_address();
		char* limit = begin + adjMap[fileindex]->get_length();
		ID from = 0, to = 0;
		ID newFrom = 0, newTo = 0;
		unsigned count = 0;
		while (begin < limit) {
			from = *(ID*) begin;
			newFrom = oldID_mapto_newID[from];
			begin += sizeof(ID);
			count = *(unsigned*) begin;
			begin += sizeof(unsigned);
			if(forward_or_backward == true)degree[newFrom].outdeg += count;
			else degree[newFrom].indeg += count;

			for (unsigned i = 0; i < count; i++) {
				to = *(ID*) begin;
				newTo = oldID_mapto_newID[to];
				if(forward_or_backward == true)degree[newTo].indeg++;
				else degree[newTo].outdeg++;
				rawFile->writeId(newFrom);
				rawFile->writeId(newTo);
				begin += sizeof(ID);
			}
		}
	}

	rawFile->close();

	TempFile *degreeFile;
	if(forward_or_backward == true)degreeFile = new TempFile(Dir + "/degFile.forward", 0);
	else degreeFile = new TempFile(Dir + "/degFile.backward", 0);
	degreeFile->write(((size_t) maxID + 1) * sizeof(Degree),(const char*) degree);
	degreeFile->close();
	delete degreeFile;
	degreeFile = NULL;
	free(degree);
	degree = NULL;

	free(oldID_mapto_newID);
	oldID_mapto_newID = NULL;
	return maxID;
}

int compquick(const void *a, const void *b) {
	return *(int *) a - *(int *) b;
}

//------------------------------------------------------------------------------------

unsigned HashID::bfs_tree_builder(ID rootID, bool *&visited, hash_map<ID, size_t> &id_offset){
	queue<ID> Q;
	Q.push(rootID);
	ID tmp_id;
	ID* addr = NULL;
	unsigned count = 0, num = 0, unique_vertices_num = 0;
	EntityIDBuffer *buffer = new EntityIDBuffer();
	buffer->setIDCount(1);
	size_t offset = 0;
	TempFile *tree_file = new TempFile(Dir + "/tree", 0);

	while(!Q.empty()){
		tmp_id = Q.front();
		Q.pop();
		if(!visited[tmp_id]){
			visited[tmp_id] = true;
			unique_vertices_num++;
			buffer->empty();
			buffer->insertID(tmp_id);
			buffer->insertID(0);
			num = 0 ;
			addr = (ID*) getOffset(tmp_id);
			if (addr == NULL)continue;
			if(tmp_id != addr[0]){
				cout<<tmp_id<<" "<<addr[0]<<endl;
				continue;
			}
			//assert(tmp_id == *(addr));
			count = *(addr + 1);
			addr += 2; //skip the id and count

			for (int i = 0; i < count; i++) {
				ID temp = *(addr + i);
				if(!visited[temp]){
					buffer->insertID(temp);
					Q.push(temp);
					num++;
				}
			}

			buffer->getBuffer()[1] = num;
			unsigned size = buffer->getSize();
			tree_file->write(size * sizeof(ID), (const char*) buffer->getBuffer());
			id_offset.insert(pair<ID, size_t>(tmp_id, offset));
			offset += size;
		}
	}

	tree_file->flush();
	delete tree_file;
	delete buffer;
	return unique_vertices_num;
}

void HashID::dfs_forward_encode(ID rootID, bool *&visited, hash_map<ID, size_t> id_offset, ID &newRootID){
	stack<ID> S;
	S.push(rootID);
	ID tmp_id;
	MemoryMappedFile *tree_map = new MemoryMappedFile();
	assert(tree_map->open((Dir+"/tree.0").c_str()));
	ID *tree = (ID*) tree_map->getBegin();
	size_t offset = 0;
	unsigned count = 0, off = 0;

	while(!S.empty()){
		tmp_id = S.top();
		S.pop();
		if(!visited[tmp_id]){
			visited[tmp_id] = true;
			setOld(tmp_id, idcount);
			setNew(idcount, tmp_id);
			idcount++;
			offset = id_offset[tmp_id];
			if(tmp_id != tree[offset])continue;
			//assert(tmp_id == tree[offset]);
			count = tree[offset+1];
			if(count == 0)continue;
			offset += 2;
			for(int i = count - 1;i >= 0;i--){
				if(!visited[tree[offset+i]])S.push(tree[offset+i]);
			}
		}
	}

	newRootID = oldID_mapto_newID[rootID];
	tree_map->close();
	delete tree_map;
}

void HashID::dfs_backward_encode(ID rootID, bool *&visited, hash_map<ID, size_t> id_offset, ID &newRootID, unsigned unique_vertices_num){
	stack<ID> S;
	S.push(rootID);
	ID tmp_id;
	MemoryMappedFile *tree_map = new MemoryMappedFile();
	tree_map->open((Dir+"/tree.0").c_str());
	if(!(tree_map->getEnd() - tree_map->getBegin())){
		//zero size
		delete tree_map;
		return;
	}
	ID *tree = (ID*) tree_map->getBegin();
	size_t offset = 0;
	unsigned count = 0, off = 0;
	idcount += unique_vertices_num;
	ID cur_id = idcount;

	while(!S.empty()){
		tmp_id = S.top();
		S.pop();
		if(!visited[tmp_id]){
			visited[tmp_id] = true;
			setOld(tmp_id, cur_id);
			setNew(cur_id, tmp_id);
			cur_id--;
			offset = id_offset[tmp_id];
			if(tmp_id != tree[offset])continue;
			//assert(tmp_id == tree[offset]);
			count = tree[offset+1];
			if(count == 0)continue;
			offset += 2;
			for(int i = count - 1;i >= 0;i--){
				S.push(tree[offset+i]);
			}
		}
	}

	newRootID = oldID_mapto_newID[rootID];
	tree_map->close();
	delete tree_map;
}

//forward_or_backward == true: means forward, otherwise means backward
void HashID::encode(bool forward_or_backward){
	if(forward_or_backward == true)idcount = 1;//id from small to large
	else idcount = 0;//id from large to small
	newID_mapto_oldID = (ID *)calloc(maxID + 1, sizeof(ID));
	oldID_mapto_newID = (ID *)calloc(maxID + 1, sizeof(ID));

	if(forward_or_backward == true)get_root_vertices(maxID);
	MemoryMappedFile *rootMap = new MemoryMappedFile();
	if(forward_or_backward == true)assert(rootMap->open((Dir+"/root.forward.0").c_str()));
	else assert(rootMap->open((Dir+"/root.backward.0").c_str()));
	unsigned rootNum = (rootMap->getEnd() - rootMap->getBegin()) / sizeof(ID);
	ID *root = (ID*) rootMap->getBegin();
	if (rootNum > 0)cout << "rootNum: " << rootNum << endl;

	unsigned off = 0;
	ID newRootID = 0;
	TempFile *newRoot;
	if(forward_or_backward == true)newRoot= new TempFile(Dir + "/new_root.forward", 0);
	else newRoot= new TempFile(Dir + "/new_root.backward", 0);

	bool *visited1 = (bool *) calloc((maxID + 1), sizeof(bool));
	bool *visited2 = (bool *) calloc((maxID + 1), sizeof(bool));
	hash_map<ID, size_t> id_offset;

	//encode the root vertices based subgraph
	if(forward_or_backward == true){
		//encode so (i.e., forward tree), start vertex is a forward_root_vertex
		unsigned unique_vertices_num = 0;
		for (int i = 0; i < rootNum; i++) {
			unique_vertices_num = bfs_tree_builder(root[i], visited1, id_offset);
			if(!unique_vertices_num)continue;
			dfs_forward_encode(root[i], visited2, id_offset, newRootID);
			newRoot->writeId(newRootID);
			hash_map<ID, size_t>().swap(id_offset);
		}
	}else{
		//encode os (i.e., backward tree), start vertex is a backward_root_vertex
		unsigned unique_vertices_num = 0;
		for (int i = 0; i < rootNum; i++) {
			unique_vertices_num = bfs_tree_builder(root[i], visited1, id_offset);
			if(!unique_vertices_num)continue;
			dfs_backward_encode(root[i], visited2, id_offset, newRootID, unique_vertices_num);
			newRoot->writeId(newRootID);
			hash_map<ID, size_t>().swap(id_offset);
		}
	}
	rootMap->close();
	delete rootMap;
	cout << "in root vertices tree based encode, idcount: " << idcount << ", maxID: " << maxID << endl;

	if (idcount < maxID) {
		//encode the loop back subgraph, which means don't have root vertices
		assert(degree);
		size_t isolated_vertices = 0;
		unsigned unique_vertices_num = 0;
		if(forward_or_backward == true){
			//forward encode
			for(ID i = start_ID[0]; i <= maxID; i++) {
				if (!isNewIDExist(i, off) && both_in_and_out_vertex(i)) {
					//encode so, loop back tree based, start vertex is a vertex in the circle
					unique_vertices_num = bfs_tree_builder(i, visited1, id_offset);
					if(!unique_vertices_num)continue;
					dfs_forward_encode(i, visited2, id_offset, newRootID);
					newRoot->writeId(newRootID);
					hash_map<ID, size_t>().swap(id_offset);
				}else if(zeroDegree(i)){
					isolated_vertices++;
				}
			}
		}else{
			//backward encode
			for(ID i = start_ID[0]; i <= maxID; i++) {
				if (!isNewIDExist(i, off) && both_in_and_out_vertex(i)) {
					//encode os, loop back tree based, start vertex is a vertex in the circle
					unique_vertices_num = bfs_tree_builder(i, visited1, id_offset);
					if(!unique_vertices_num)continue;
					dfs_backward_encode(i, visited2, id_offset, newRootID, unique_vertices_num);
					newRoot->writeId(newRootID);
					hash_map<ID, size_t>().swap(id_offset);
				}else if(zeroDegree(i)){
					isolated_vertices++;
				}
			}
		}
		cout<<"in loop back tree based, idcount: "<<idcount<<", maxID: "<<maxID
				<<", isolated_vertices: "<<isolated_vertices<<endl;
	}

	if(OSFile::FileExists((Dir+"/tree.0").c_str()))OSFile::FileDelete((Dir+"/tree.0").c_str());
	free(visited1);
	free(visited2);

	if (idcount < maxID) {
		//vertices that haven't encode yet
		for(ID i = start_ID[0]; i <= maxID; i++){
			if (!isNewIDExist(i, off) && !zeroDegree(i)){
				setOld(i, idcount);
				setNew(idcount, i);
				newRoot->writeId(idcount);
				idcount++;
			}
		}
		cout<<"in total, idcount: "<<idcount<<", maxID: "<<maxID<<endl;
	}

	newRoot->flush();
	delete newRoot;
	save_encoded_ids(forward_or_backward);
}

void HashID::save_encoded_ids(bool forward_or_backward){
	TempFile *old_mapto_new, *new_mapto_old;
	if(forward_or_backward == true){
		remove((Dir+"/root.forward.0").c_str());
		old_mapto_new = new TempFile(Dir + "/old_mapto_new.forward", 0);
		new_mapto_old = new TempFile(Dir + "/new_mapto_old.forward", 0);
	}
	else{
		remove((Dir+"/root.backward.0").c_str());
		maxID = idcount;
		old_mapto_new = new TempFile(Dir + "/old_mapto_new.backward", 0);
		new_mapto_old = new TempFile(Dir + "/new_mapto_old.backward", 0);
	}

	old_mapto_new->write((maxID + 1) * sizeof(ID), (const char*) oldID_mapto_newID);
	old_mapto_new->close();
	delete old_mapto_new;

	new_mapto_old->write((maxID + 1) * sizeof(ID), (const char*) newID_mapto_oldID);
	new_mapto_old->close();
	delete new_mapto_old;

	free(newID_mapto_oldID);
	newID_mapto_oldID = NULL;
	cout << "end write file old_mapto_new and new_mapto_old, size: " << (maxID + 1) * sizeof(ID) << endl;
}

