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

#include "BuildAdjGraph.h"

const unsigned BuildAdjGraph::maxBucketSize = 96;
//const unsigned BuildAdjGraph::maxThreadSize = 32;
int BuildAdjGraph::flag = 0;

BuildAdjGraph::BuildAdjGraph() {
	// TODO Auto-generated constructor stub
	matrixMap = NULL;
	maxID = 0;
	memset(start_ID, 0, sizeof(start_ID));
}

BuildAdjGraph::~BuildAdjGraph() {
	// TODO Auto-generated destructor stub
	if (matrixMap)delete matrixMap;
	matrixMap = NULL;
}

unsigned BuildAdjGraph::convertToAdj(const char* inFile, const char* outDir,
		const char* outName, int pos) {
	flag = pos;

	MemoryMappedFile mappedIn;
	assert(mappedIn.open(inFile));
	cout << "edge file " << inFile << endl;
	const ID* begin = (ID*) mappedIn.getBegin(), *limit =
			(ID*) mappedIn.getEnd();
	const ID * reader = begin;
	size_t inSize = limit - reader;
	assert(inSize > 6);
	unsigned bucketSize = 0;
	if (inSize < 1000)
		bucketSize = 2;
	else
		bucketSize = maxBucketSize;

	size_t length = inSize / bucketSize;
	length = length + (length % 2 == 0 ? 0 : 1);

	const ID* start_Pos[maxBucketSize];
	vector<ID> preList;
	unsigned x = 0, y = 0;
	unsigned i = 0;

	x = *(reader + pos);
	start_ID[i] = x;
	start_Pos[i] = reader;
	preList.push_back(i++);
	reader += length;

	while (reader < limit) {
		x = *(reader + pos);

		while (reader + 8 < limit) {
			reader += 2;
			y = *(reader + pos);
			if (y != x)
				break;
		}
		start_ID[i] = y;
		start_Pos[i] = reader;
		preList.push_back(i++);
		reader += length;
	}
	maxID = *((ID*) limit - 2 + pos);

	if (matrixMap == NULL) {
		matrixMap = new Matrixmap(outDir, (char*) outName, preList);
	}
	assert(matrixMap != NULL);

	unsigned j = 0;
	TempFile* startFile = new TempFile(string(outDir) + "/" + outName + "/startID", 0);
	for (j = 0; j < i; j++) {
		startFile->writeId(start_ID[j]);
	}
	startFile->writeId(maxID + 1);
	startFile->close();
	delete startFile;

	cout << "bucket num:" << i << endl;

	for (j = 0; j < i; j++) {
		BucketManager* buc = matrixMap->getBucketManager(j);
		if (j + 1 < i) {
			insertTask(start_Pos[j], start_Pos[j + 1], buc);
		} else {
			insertTask(start_Pos[j], limit, buc);
		}
	}

	mappedIn.close();
	return bucketSize;
}

void BuildAdjGraph::insertTask(const ID* begin, const ID* limit,
		BucketManager* buc) {
	unsigned x = 0, y = 0, lastx = 0;
	EntityIDBuffer yList;
	if (flag == 1) {
		x = *(begin + 1);
		lastx = x;
		yList.insertID(*begin);
		begin += 2;

		while (begin < limit) {
			x = *(begin + 1);
			if (x == lastx) {
				yList.insertID(*begin);
			} else {
				buc->insertNewXY(lastx, &yList);
				yList.empty();
				yList.insertID(*begin);
				lastx = x;
			}

			begin += 2;

		}
		assert(lastx == x);
		buc->insertNewXY(x, &yList);
	} else if (flag == 0) {
		x = *(begin);
		lastx = x;
		yList.insertID(*(begin + 1));
		//cout <<x <<"==" <<*(begin+1) << endl;
		begin += 2;

		while (begin < limit) {
			x = *(begin);
			//cout <<x <<"==" <<*(begin+1) << endl;
			if (x == lastx) {
				yList.insertID(*(begin + 1));
			} else {
				buc->insertNewXY(lastx, &yList);
				yList.empty();
				yList.insertID(*(begin + 1));
				lastx = x;
			}

			begin += 2;

		}
		assert(lastx == x);
		buc->insertNewXY(x, &yList);
	}
	buc->endNewInsert();
}
