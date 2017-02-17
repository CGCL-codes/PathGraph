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

int cilk_main(int argc, char* argv[])
{
	if(argc != 2) {
		fprintf(stderr, "Usage: %s <Database Directory>\n", argv[0]);
		return -1;
	}

	struct timeval start_time, end_time;
	gettimeofday(&start_time, NULL);
	AppExample* app_exam = new AppExample(false, false, true);
	if(app_exam == NULL) {
		cerr << "create failed!" << endl;
		return -1;
	}
	string matrix = "Matrixso/";
	app_exam->init(argv[1],matrix.c_str(),false);

	unsigned bucsize = app_exam->getBucSize();
	unsigned maxID = app_exam->getMaxID();
	double* res = (double *)calloc(maxID+1,sizeof(double));
	assert(res);

	if(app_exam->small_graph()){
		cilk_for(int i = 0; i< bucsize; i++){
			app_exam->spmv(i,res);
		}
		cout<<"(cilk_for) spmv finished"<<endl;
	}else{
		app_exam->spmv(res);
		cout<<"(work stealing) spmv finished"<<endl;
	}
	gettimeofday(&end_time, NULL);
	cout<<"spmv compute time elapse: "<<((end_time.tv_sec - start_time.tv_sec) * 1000000 + (end_time.tv_usec - start_time.tv_usec)) / 1000000.0<<" s"<<endl;
	
	cout<<"start to write the results of spmv"<<endl;
	TempFile* tempFile = new TempFile(string(argv[1])+"/spmv");
	tempFile->write((maxID+1)*sizeof(double),(char*)res);
	tempFile->close();
	delete tempFile;
	cout<<"finish writing the results"<<endl;

	free(res);
	delete app_exam;
	gettimeofday(&end_time, NULL);
	cout<<"spmv time elapse: "<<((end_time.tv_sec - start_time.tv_sec) * 1000000 + (end_time.tv_usec - start_time.tv_usec)) / 1000000.0<<" s"<<endl;

	return 0;
}

