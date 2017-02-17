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
#include <signal.h>
#include <execinfo.h>
#include "MMapBuffer.h"
#include "AppExample.h"
#include <vector>

void handleSegmentFault(int sig) {
	void* array[1024];
	size_t size;
	size = backtrace(array, sizeof(array) / sizeof(void*));
	backtrace_symbols_fd(array, size, fileno(stderr));
	abort();
	return;
}

int cilk_main(int argc, char* argv[]) {
	if(argc != 4) {
		fprintf(stderr, "Usage: %s <Database Directory> <iterate times> <printtop>\n", argv[0]);
		return -1;
	}
//	signal(SIGSEGV, handleSegmentFault);

	struct timeval start_time, end_time, end_time1;
	gettimeofday(&start_time, NULL);
	AppExample *app_exam = new AppExample(true, false, true);
	string matrix = "Matrixos/";
	app_exam->init(argv[1], matrix.c_str(), true);
	gettimeofday(&end_time1, NULL);
	cout<<"load time cost:"<<((end_time1.tv_sec - start_time.tv_sec) * 1000000 + (end_time1.tv_usec - start_time.tv_usec)) / 1000000.0<<" s"<<endl;

	unsigned size = app_exam->getBucSize();
	unsigned maxID = app_exam->getMaxID();
	unsigned itera = atoi(argv[2]);

	//in itera 0, use cilk_for rather than work stealing to initialize the vertices,
	//for the partition tasks need little time, no need to split them into small chunk tasks
	cilk_for (unsigned i = 0; i < size; i++) {
		app_exam->pagerank(i, 0);
	}
	app_exam->endComputePageRank();
	cout << "(cilk_for) end itera 0" << endl;

	if(app_exam->small_graph()){
		//small graph means little tasks, so no need to split the partition tasks into chunk tasks
		for (unsigned j = 1; j < itera; j++){
			cilk_for(unsigned i = 0; i < size; i++) {
				app_exam->pagerank(i, j);
			}
			app_exam->endComputePageRank();
			cout << "(cilk_for) end itera " << j << endl;
		}
	}
	else{
		//split partition tasks into chunk tasks, then use work stealing as a scheduler
		for (unsigned j = 1; j < itera; j++){
			app_exam->pagerank(j);
			app_exam->endComputePageRank();
			cout << "(work stealing) end itera " << j << endl;
		}
	}
	app_exam->save_rank();
	//app_exam->save(string(argv[1])+"/pagerank.0", app_exam->get_vertices_rank(), (maxID+1)*sizeof(float));

	cout << "compute over" << endl;
	app_exam->getMaxValue(atoi(argv[3]));
	delete app_exam;
	gettimeofday(&end_time, NULL);
	cout<<"run time cost:"<<((end_time.tv_sec - start_time.tv_sec) * 1000000 + (end_time.tv_usec - start_time.tv_usec)) / 1000000.0<<" s"<<endl;

	return 0;
}
