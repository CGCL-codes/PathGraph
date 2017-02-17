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

int main(int argc, char* argv[]){
    if(argc != 4) {
        fprintf(stderr, "Usage: %s <Dataset> <Database Directory> <Vertex Number>\n", argv[0]);
        return -1;
    }
    signal(SIGSEGV, handleSegmentFault);

    string Dir = argv[2];
    if(OSFile::DirectoryExists(argv[2]) == false) {
        OSFile::MkDir(argv[2]);
    }

    struct timeval start_time, end_time, tmp_start, tmp_end;
    gettimeofday(&start_time, NULL);

    TempFile* rawFile = new TempFile(Dir+"/rawTest");
    string path = argv[1];
    {
        HashID* hi = new HashID(Dir);
        hi->convert_adj_nonum(path, atoi(argv[3]));
        hi->init(true);
        hi->encode(true);
        hi->convertToRaw(rawFile, true);
        delete hi;
    }
    gettimeofday(&tmp_end, NULL);
    cout<<"encode_so time elapse:"<<((tmp_end.tv_sec - start_time.tv_sec) * 1000000 + (tmp_end.tv_usec - start_time.tv_usec)) / 1000000.0<<" s"<<endl;

    cout<<"--------------------------------"<<endl;
    gettimeofday(&tmp_start, NULL);
    cout <<"start build Matrixso" <<endl;
    TempFile* rawso =  new TempFile(Dir+"/rawTest.so");
    Sorter::sort(*rawFile,*rawso,TempFile::skipIdId,TempFile::compare12);
    BuildAdjGraph* bua = new BuildAdjGraph();
    bua->convertToAdj(rawso->getFile().c_str(),Dir.c_str(),"Matrixso",0);
    rawFile->discard();
    rawso->discard();
    delete rawFile;
    delete rawso;
    delete bua;
    cout <<"finish building Matrixso" <<endl;
    gettimeofday(&tmp_end, NULL);
    cout<<"build_so time elapse:"<<((tmp_end.tv_sec - tmp_start.tv_sec) * 1000000 + (tmp_end.tv_usec - tmp_start.tv_usec)) / 1000000.0<<" s"<<endl;

    cout<<"--------------------------------"<<endl;
    gettimeofday(&tmp_start, NULL);
    rawFile = new TempFile(Dir+"/rawTest");
    path = argv[1];
    {
        HashID* hi = new HashID(Dir);
        hi->convert_adj_to_reverse_adj(path, atoi(argv[3]));
        hi->init(false);
        hi->encode(false);
        hi->convertToRaw(rawFile, false);
        delete hi;
    }
    gettimeofday(&tmp_end, NULL);
    cout<<"encode_os time elapse:"<<((tmp_end.tv_sec - tmp_start.tv_sec) * 1000000 + (tmp_end.tv_usec - tmp_start.tv_usec)) / 1000000.0<<" s"<<endl;

    cout<<"--------------------------------"<<endl;
    gettimeofday(&tmp_start, NULL);
    cout <<"start build Matrixos" <<endl;
    TempFile* rawos =  new TempFile(Dir+"/rawTest.os");
    Sorter::sort(*rawFile,*rawos,TempFile::skipIdId,TempFile::compare12);
    BuildAdjGraph* bub = new BuildAdjGraph();
    bub->convertToAdj(rawos->getFile().c_str(),Dir.c_str(),"Matrixos",0);
    rawFile->discard();
    rawos->discard();
    delete rawFile;
    delete rawos;
    delete bub;
    cout <<"finish building Matrixos" <<endl;
    gettimeofday(&tmp_end, NULL);
    cout<<"build_os time elapse:"<<((tmp_end.tv_sec - tmp_start.tv_sec) * 1000000 + (tmp_end.tv_usec - tmp_start.tv_usec)) / 1000000.0<<" s"<<endl;
    cout<<"--------------------------------"<<endl;
    gettimeofday(&end_time, NULL);
    cout<<"build time elapse:"<<((end_time.tv_sec - start_time.tv_sec) * 1000000 + (end_time.tv_usec - start_time.tv_usec)) / 1000000.0<<" s"<<endl;
    return 0;
}
