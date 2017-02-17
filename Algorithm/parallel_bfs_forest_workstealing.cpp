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

#include "TripleBit.h"
#include "OSFile.h"
#include "MMapBuffer.h"
#include "EntityIDBuffer.h"
#include "AppExample.h"
#include "MatrixMapOp.h"
#include "TempFile.h"

int cilk_main(int argc, char* argv[])
{
	if(argc != 2) {
		fprintf(stderr, "Usage: %s <Database Directory>\n", argv[0]);
		return -1;
	}

	struct timeval start_time, end_time;
	gettimeofday(&start_time, NULL);
	AppExample* app_exam = new AppExample(false, true, false);
	if(app_exam == NULL) {
		cout << "create failed!" << endl;
		return -1;
	}
	string matrix = "Matrixso/";
	app_exam->init(argv[1],matrix.c_str(),true);

	unsigned maxID = app_exam->getMaxID();
	bool* isVisited = (bool*)calloc(maxID+1,sizeof(bool));
	MemoryMappedFile *rootFile = new MemoryMappedFile();
	assert(rootFile->open((string(argv[1])+"/new_root.forward.0").c_str()));
	unsigned rootNum = (rootFile->getEnd() - rootFile->getBegin()) / sizeof(ID);
	ID *array = (ID*) rootFile->getBegin();

	if(app_exam->small_graph() || rootNum <= SPECIAL_ROOT_SIZE){
		cilk_for(int i = 0; i< rootNum; i++){
			app_exam->bfs_forest(array[i], isVisited);
		}
		cout<<"(cilk_for) bfs_forest finished"<<endl;
	}else{
		app_exam->bfs_forest(rootNum, array, isVisited);
		cout<<"(work stealing) bfs_forest finished"<<endl;
	}

	rootFile->close();
	delete rootFile;
	free(isVisited);
	delete app_exam;
	gettimeofday(&end_time, NULL);
	cout<<"parallel bfs forest time elapse: "<<((end_time.tv_sec - start_time.tv_sec) * 1000000 + (end_time.tv_usec - start_time.tv_usec)) / 1000000.0<<" s"<<endl;
	return 0;
}
