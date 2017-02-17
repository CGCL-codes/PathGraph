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
#include "AppExample.h"
#include "EntityIDBuffer.h"

int print_top_n(EntityIDBuffer* cc_count, string dir, int printtop){
	cc_count->sort(2);
	string outName = dir + "/components.txt";
	FILE *resf = fopen(outName.c_str(), "w");
	if(resf == NULL){
		cerr<<"create file components.txt error"<<endl;
		return -1;
	}

	fprintf(resf, "component: count\n");
	ID *p = cc_count->getBuffer();
	size_t cc_size = cc_count->getSize();
	for(long i = 2 * cc_size - 1;i >= 1;i-=2){
		fprintf(resf,"%u: %u\n", p[i-1], p[i]);
	}
	fclose(resf);

	cout<<"total number of connected-components: "<<cc_size<<endl;
	cout<<"List of labels was written to file: "<<outName<<endl;
	cout<<"top "<<printtop<<" follows:"<<endl;
	for(long i = 0, index = 2 * cc_size - 1;i < (long)std::min((size_t)printtop, cc_size);i++, index-=2){
		cout<<(i+1)<<". label: "<<p[index-1]<<" , size: "<<p[index]<<endl;
	}

	return 1;
}

int cilk_main(int argc, char* argv[])
{
	if(argc != 3) {
		fprintf(stderr, "Usage: %s <Database Directory> <printtop>\n", argv[0]);
		return -1;
	}

	struct timeval start_time, end_time;
	gettimeofday(&start_time, NULL);
	AppExample *app_exam = new AppExample(false, false, false);
	if(app_exam == NULL){
		cerr << "create failed" << endl;
		return -1;
	}
	string matrix = "Matrixso/";
	app_exam->init(argv[1], matrix.c_str(), true);

	int printtop = atoi(argv[2]);
	unsigned maxID = app_exam->getMaxID();
	bool *isVisited = (bool *)calloc(maxID + 1, sizeof(bool));
	ID *idValue = (ID *)calloc(maxID + 1, sizeof(ID));
	ID *changes = (ID *)calloc(maxID + 1, sizeof(ID));
	for(unsigned i = 0;i <= maxID;i++){
		changes[i] = i;
		idValue[i] = i;
	}

	MemoryMappedFile *rootFile = new MemoryMappedFile();
	assert(rootFile->open((string(argv[1])+"/new_root.forward.0").c_str()));
	unsigned rootNum = (rootFile->getEnd() - rootFile->getBegin()) / sizeof(ID);
	ID *array = (ID*) rootFile->getBegin();

	cilk_for(int i = 0;i < rootNum;i++){
	//for(int i = 0;i < rootNum;i++){
		app_exam->wcc(array[i],isVisited, idValue, changes);
	}
	app_exam->wcc_apply_changes(idValue, changes, maxID+1,true);
	cout<<"(cilk_for) wcc finished"<<endl;

	ID *distinct_labels = changes;
	memset(distinct_labels, 0, (maxID + 1) * sizeof(ID));
	for(unsigned i = 1;i <= maxID;i++)distinct_labels[idValue[i]]++;

	EntityIDBuffer *cc_count = new EntityIDBuffer();
	cc_count->setIDCount(2);
	cc_count->setSortKey(3);//unsorted
	for(unsigned i = 0;i <= maxID;i++){
		if(distinct_labels[i]){
			cc_count->insertID(i);
			cc_count->insertID(distinct_labels[i]);
		}
	}
	free(distinct_labels);
	print_top_n(cc_count, argv[1], printtop);

	gettimeofday(&end_time, NULL);
	cout<<"wcc time elapse: "<<((end_time.tv_sec - start_time.tv_sec) * 1000000 + (end_time.tv_usec - start_time.tv_usec)) / 1000000.0<<" s"<<endl;

	rootFile->close();
	delete rootFile;
	delete cc_count;
	free(isVisited);
	free(idValue);
	delete app_exam;
	return 0;
}
