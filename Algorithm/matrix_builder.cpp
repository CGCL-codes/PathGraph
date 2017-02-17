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
#include "HashID.h"
#include "BuildAdjGraph.h"
#include "DebugTimer.h"
#include "Sorter.h"
#include "OSFile.h"

void handleSegmentFault(int sig) {
    void*  array[1024];
    size_t size;
    size = backtrace(array, sizeof(array) / sizeof(void*));
    backtrace_symbols_fd(array, size, fileno(stderr));
    abort();
    return;
}

int cilk_main(int argc, char* argv[]){
	if(argc != 4) {
		fprintf(stderr, "Usage: %s <Dataset> <Database Directory> <Vertex Number>\n", argv[0]);
		return -1;
	}
    signal(SIGSEGV, handleSegmentFault);
	
	string Dir = argv[2];
	if(OSFile::DirectoryExists(argv[2]) == false) {
		OSFile::MkDir(argv[2]);
	}
	
	struct timeval start_time, end_time;
	gettimeofday(&start_time, NULL);
	TempFile* rawFile = new TempFile(Dir+"/rawTest");
	TempFile* rawos =  new TempFile(Dir+"/rawTest.os");
	string path = argv[1];
	{
		DebugTimer timer("DFS = %.2fs\n");
		DebugTimerObject _(timer);
		HashID* hi = new HashID(Dir);
		hi->convert_adj_nonum(path, atoi(argv[3]));
		hi->init();
		unsigned len = hi->getAdjNum();
		cout <<"len:" << len << endl;
		cilk_for( unsigned i = 0;i <= len;i++){
			hi->sort_degree(i);
		}
		cout <<"end all sort"<< endl;
		hi->DFS(true);
    
		hi->convertToRaw(rawFile);
		delete hi;
		rename(string(Dir+"/degFile.forward.0").c_str(), string(Dir+"/degFile.backward.0").c_str());
	}
    
    DebugTimer timer("Sorter = %f.2fs\n");
    DebugTimerObject _(timer);

	cout<<"--------------------------------"<<endl;
	cout <<"start build Matrixos" <<endl;
    Sorter::sort(*rawFile,*rawos,TempFile::skipIdId,TempFile::compare21);
    BuildAdjGraph* bua = new BuildAdjGraph();
    bua->convertToAdj(rawos->getFile().c_str(),Dir.c_str(),"Matrixos",1);
    delete bua;

	cout<<"--------------------------------"<<endl;
    cout <<"start build Matrixso" <<endl;
    TempFile* rawso =  new TempFile(Dir+"/rawTest.so");
    Sorter::sort(*rawFile,*rawso,TempFile::skipIdId,TempFile::compare12);
    BuildAdjGraph* bub = new BuildAdjGraph();
    bub->convertToAdj(rawso->getFile().c_str(),Dir.c_str(),"Matrixso",0);
    delete bub;
	
    rawos->discard();
	rawFile->discard();
	rawso->discard();
    delete rawos;
    delete rawso;
    delete rawFile;

	gettimeofday(&end_time, NULL);
	cout<<"--------------------------------"<<endl;
	cout<<"build time elapse:"<<((end_time.tv_sec - start_time.tv_sec) * 1000000 + (end_time.tv_usec - start_time.tv_usec)) / 1000000.0<<" s"<<endl;
	return 0;
}
