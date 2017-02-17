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

	for (unsigned j = 0; j < itera; j++) {
		cilk_for (unsigned i = 0; i < size; i++) {
			app_exam->pagerank(i, j);
		}
		app_exam->endComputePageRank();
		cout << "(cilk_for) end itera " << j << endl;
		/*if(j >= 1){
			double sum = app_exam->sum_of_changes();
        	cout<<"compared with iterate "<<j-1<<", total square deviation changes of iterate "<<j<<" is: "<<sum<<endl;
       	}
        app_exam->save_rank();*/
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
