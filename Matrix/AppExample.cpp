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

#include "AppExample.h"
#include "OSFile.h"
#include <cmath>

const unsigned AppExample::maxRunChunk = 3;

AppExample::AppExample(bool is_pagerank_not_spmv, bool is_bfs_not_wcc, bool scheduler_choice) {
	// TODO Auto-generated constructor stub
	maxID = 0;
	memset(start_ID, 0, sizeof(start_ID));
	isMemory = true;
	this->is_pagerank_not_spmv = is_pagerank_not_spmv;
	this->is_bfs_not_wcc = is_bfs_not_wcc;
	idValueIn = NULL;
	idValueOut = NULL;
	idValueMap = NULL;
	degree = NULL;
	degreeMap = NULL;
	root_array = NULL;
	isVisited = NULL;
	idValue = NULL;
	changes = NULL;
	scheduler = new Scheduler(scheduler_choice);
#ifdef TASK_ANALYSIS
	pthread_mutex_init(&print_mutex, NULL);
#endif
}

AppExample::~AppExample() {
	// TODO Auto-generated destructor stub
	map<char, Matrixmap*>::iterator iter = matrixMap.begin();
	for (; iter != matrixMap.end(); iter++) {
		delete iter->second;
		iter->second = NULL;
	}
	matrixMap.clear();

	if(this->is_pagerank_not_spmv){
		if(isMemory){
			if(idValueIn != NULL)free(idValueIn);
			idValueIn = NULL;
			if(idValueOut != NULL)free(idValueOut);
			idValueOut = NULL;

			if (degree != NULL)free(degree);
			degree = NULL;
		}else{
			if(idValueMap != NULL)delete idValueMap;
			idValueMap = NULL;
			idValueIn = NULL;

			if (degreeMap != NULL)delete degreeMap;
			degreeMap = NULL;
			degree = NULL;
		}
	}

	delete scheduler;
#ifdef TASK_ANALYSIS
	pthread_mutex_destroy(&print_mutex);
	map<unsigned long, unsigned long>::iterator it = thread_info.begin(), limit = thread_info.end();
	for(;it != limit;it++)cout<<it->first<<" "<<it->second * 1.0 / 1000<<" ms"<<endl;
	map<unsigned long, unsigned long>().swap(thread_info);
#endif
}

Status AppExample::init(const char* outDir, const char* outName, bool isMemory){
	this->Dir = string(outDir);
	this->Name = string(outName);
	this->isMemory = isMemory;

	if (matrixMap[1] == NULL) {
		matrixMap[1] = Matrixmap::parallel_load(outDir, outName, isMemory);
		//matrixMap[1] = Matrixmap::load(outDir, outName, isMemory);
	}
	cout << "load matrix" << endl;

	unsigned size = 0;
	const char* startIDPath = (Dir + "/" + outName + "/startID.0").c_str();
	if (OSFile::FileExists(startIDPath)) {
		char* buf = NULL;
		size = HashID::loadFileinMemory(startIDPath, buf);
		memcpy(start_ID, buf, size);
		free(buf);
	} else {
		cerr << "startID file not exit!" << endl;
		assert(false);
	}

	size = size / sizeof(ID) - 1;
	bucSize = size;
	maxID = start_ID[size] - 1;

	if(this->is_pagerank_not_spmv){
		if (isMemory) {
			idValueIn = (float*) calloc(maxID + 1, sizeof(float));
			assert(idValueIn);
			idValueOut = (float*) calloc(maxID + 1, sizeof(float));
			assert(idValueOut);
		}else{
			char tempFile[150];
			string idValueFile = Dir + "/" + outName + "/idValue";
			idValueMap = new MMapBuffer(idValueFile.c_str(), (maxID + 1) * sizeof(float));
			idValueIn = (float*) idValueMap->getBuffer();
		}

		if (degree == NULL) {
			const char* degreeFilePath = (Dir + "/degFile.backward.0").c_str();
			if (isMemory) {
				char* buf = NULL;
				HashID::loadFileinMemory(degreeFilePath, buf);
				//HashID::parallel_load_inmemory(degreeFilePath, buf);
				degree = (Degree*) buf;
			} else {
				degreeMap = new MMapBuffer(degreeFilePath, 0);
				degree = (Degree*) degreeMap->getBuffer();
			}
			cout << "load degree" << endl;
		}
	}

	cout << "load complete!" << endl;
	return OK;
}

//---------------------------------cilk application------------------------------------

Status AppExample::pagerank(unsigned index, unsigned itera) {
#ifdef TASK_ANALYSIS
	struct timeval start_time, end_time;
	gettimeofday(&start_time, NULL);
#endif
	BucketManager* buc = matrixMap[1]->getBucketManager(index);
	const uchar* reader = buc->getStartPtr();
	const uchar* limit = buc->getEndPtr();
	float sum = 0;

	if (itera == 0) {
		memset(idValueOut+start_ID[index], RANDOMRESETPROB, start_ID[index + 1] - start_ID[index]);
	} else if (itera == 1) {
		ID x;
		int size = 0;
		int len, idoff, edgeoff, lastedgeoff;

		const uchar* base = reader - sizeof(BucketManagerMeta);
		while (reader < limit) {
			BucketManager::getTwo(reader, size);
			if (size == 0) {
				idoff = 2;
				x = *(ID*) (reader + idoff);
				if(x == 0)break;
				idoff += sizeof(ID);
				len = *(unsigned*) (reader + idoff);
				idoff += 4;
				reader += idoff;
				const uchar* end = reader + len;
				sum = 0;
				ID y0 = 0, yi = 0;

				//read the first y (i.e., y0)
				y0 = BucketManager::readYi(reader, y0, end);
				assert(degree[y0].outdeg);
				sum += 1.0 / degree[y0].outdeg;

				//read yi
				yi = y0;
				while(reader < end){
					//yi = BucketManager::readYi(reader, y0, end);
					yi = BucketManager::readYi(reader, yi, end);
					assert(degree[yi].outdeg);
					sum += 1.0 / degree[yi].outdeg;
				}

				idValueOut[x] = RANDOMRESETPROB + (1 - RANDOMRESETPROB) * sum;
				base += (size_t) ceil((double) (len + 10) / BucketManager::pagesize) * BucketManager::pagesize;
			} else {
				idoff = 2;
				lastedgeoff = 2 + size * (2 + sizeof(ID));
				const uchar* tempReader = reader;
				reader += lastedgeoff;
				for (int i = 0; i < size; i++) {
					x = *(ID*) (tempReader + idoff);
					idoff += sizeof(ID);
					BucketManager::getTwo(tempReader + idoff, edgeoff);
					idoff += 2;
					if (edgeoff + 1 <= lastedgeoff) {
						cout << edgeoff + 1 << "  " << lastedgeoff << endl;
						exit(-1);
					}

					const uchar* end = reader + (edgeoff + 1 - lastedgeoff);
					sum = 0;
					ID y0 = 0, yi = 0;

					//read the first y (i.e., y0)
					y0 = BucketManager::readYi(reader, y0, end);
					assert(degree[y0].outdeg);
					sum += 1.0 / degree[y0].outdeg;

					//read yi
					yi = y0;
					while(reader < end){
						//yi = BucketManager::readYi(reader, y0, end);
						yi = BucketManager::readYi(reader, yi, end);
						assert(degree[yi].outdeg);
						sum += 1.0 / degree[yi].outdeg;
					}

					idValueOut[x] = RANDOMRESETPROB + (1 - RANDOMRESETPROB) * sum;
					lastedgeoff = edgeoff + 1;
				}
				base += BucketManager::pagesize;
			}
			reader = base;
		}
	} else {
		ID x;
		int size = 0;
		int len, idoff, edgeoff, lastedgeoff;

		const uchar* base = reader - sizeof(BucketManagerMeta);
		while (reader < limit) {
			BucketManager::getTwo(reader, size);
			if (size == 0) {
				idoff = 2;
				x = *(ID*) (reader + idoff);
				if(x == 0)break;
				idoff += sizeof(ID);
				len = *(unsigned*) (reader + idoff);
				idoff += 4;
				reader += idoff;
				const uchar* end = reader + len;
				sum = 0;
				ID y0 = 0, yi = 0;

				//read the first y (i.e., y0)
				y0 = BucketManager::readYi(reader, y0, end);
				sum += idValueIn[y0] / degree[y0].outdeg;

				//read yi
				yi = y0;
				while(reader < end){
					//yi = BucketManager::readYi(reader, y0, end);
					yi = BucketManager::readYi(reader, yi, end);
					sum += idValueIn[yi] / degree[yi].outdeg;
				}

				idValueOut[x] = RANDOMRESETPROB + (1 - RANDOMRESETPROB) * sum;
				base += (size_t) ceil((double) (len + 10) / BucketManager::pagesize) * BucketManager::pagesize;
			} else {
				idoff = 2;
				lastedgeoff = 2 + size * (2 + sizeof(ID));
				const uchar* tempReader = reader;
				reader += lastedgeoff;
				for (int i = 0; i < size; i++) {
					x = *(ID*) (tempReader + idoff);
					idoff += sizeof(ID);
					BucketManager::getTwo(tempReader + idoff, edgeoff);
					idoff += 2;
					if (edgeoff + 1 <= lastedgeoff) {
						cout << edgeoff + 1 << "  " << lastedgeoff << endl;
						exit(-1);
					}

					const uchar* end = reader + (edgeoff + 1 - lastedgeoff);
					sum = 0;
					ID y0 = 0, yi = 0;

					//read the first y (i.e., y0)
					y0 = BucketManager::readYi(reader, y0, end);
					sum += idValueIn[y0] / degree[y0].outdeg;

					//read yi
					yi = y0;
					while(reader < end){
						//yi = BucketManager::readYi(reader, y0, end);
						yi = BucketManager::readYi(reader, yi, end);
						sum += idValueIn[yi] / degree[yi].outdeg;
					}

					idValueOut[x] = RANDOMRESETPROB + (1 - RANDOMRESETPROB) * sum;
					lastedgeoff = edgeoff + 1;
				}
				base += BucketManager::pagesize;
			}
			reader = base;
		}
	}

#ifdef TASK_ANALYSIS
	if(itera > 0){
		gettimeofday(&end_time, NULL);
		pthread_mutex_lock(&print_mutex);
		cout<<pthread_self()<<" "<<((end_time.tv_sec - start_time.tv_sec) * 1000000 + (end_time.tv_usec - start_time.tv_usec)) / 1000.0<<" ms"<<endl;
		pthread_mutex_unlock(&print_mutex);
	}
#endif
	return OK;
}

void AppExample::endComputePageRank() {
	float *tmp = idValueOut;
	idValueOut = idValueIn;
	idValueIn = tmp;
}

Status AppExample::save_rank(){
	if (isMemory) {
		string path = Dir + "/pagerank";
		TempFile *result = new TempFile(path, 0);
		result->write((maxID + 1) * sizeof(float),(const char*) idValueIn);
		result->close();
		delete result;
	} else {
		idValueMap->flush();
	}

	return OK;
}

void AppExample::save_task(char *dst, char *src, size_t copysize){
	memcpy(dst, src, copysize);
}

Status AppExample::save(string path, char *data, size_t size){
	int task_num = size / MEMCPY_SIZE, remain = size % MEMCPY_SIZE;
	size_t begin = 0;
	MMapBuffer *buf = new MMapBuffer(path.c_str(), size);
	char *dst = buf->get_address();

	if(task_num){
		for(int i = 0;i < task_num;i++){
			CThreadPool::getInstance().AddTask(boost::bind(&AppExample::save_task, this, dst+begin, data+begin, MEMCPY_SIZE));
			begin += MEMCPY_SIZE;
		}
		CThreadPool::getInstance().Wait();
	}
	if(remain)memcpy(dst+begin, data+begin, remain);
	buf->flush();
	delete buf;

	return OK;
}

double AppExample::sum_of_changes(){
	MemoryMappedFile *rankFile = new MemoryMappedFile();
	rankFile->open((Dir+"/pagerank.0").c_str());
	float *rank = (float *)rankFile->getBegin();
	double sum = 0, sum2 = 0;
	size_t index = 0;
	float max_diff = 0;

	for(size_t i = 0;i <= maxID;i++, index++){
		sum2 += fabs(idValueIn[i] - rank[index]);
		sum += pow((double)fabs(idValueIn[i] - rank[index]), 2.0);
		max_diff = std::max(fabs(idValueIn[i] - rank[index]), max_diff);
	}

	rankFile->close();
	delete rankFile;
	cout<<"sum of changes: "<<sum2<<endl;
	cout<<"index: "<<index<<endl;
	cout<<"max_diff: "<<max_diff<<endl;
	return sum / index;
}

int comp(const void *a, const void *b) {
	if(((VV*) a)->first - ((VV *) b)->first >= 0.001)
		return 1;
	else
		return -1;
}

void AppExample::getMaxValue(unsigned num) {
	TopValue* topValue[BuildAdjGraph::maxBucketSize];
	VV* result[BuildAdjGraph::maxBucketSize];
	int i = 0;
	for (i = 0; i < bucSize; i++) {
		topValue[i] = new TopValue(idValueIn + start_ID[i],start_ID[i], start_ID[i + 1] - start_ID[i], num, result[i]);
	}

	for (i = 0; i < bucSize; i++) {
		delete topValue[i];
		topValue[i] = NULL;
	}

	VV* runs = result[0];
	unsigned index = 1;
	unsigned pos = 0, size = num;
	for (index = 1; index < bucSize; index++) {
		for (i = 0; i < num; i++) {
			if (result[index][i].first <= runs[0].first)
				continue;
			runs[0] = result[index][i];
			pos = 0, size = num;
			while (pos < size) {
				unsigned left = 2 * pos + 1, right = left + 1;
				if (left >= size)
					break;
				if (right < size) {
					if (runs[pos].first > runs[left].first) {
						if (runs[pos].first > runs[right].first) {
							if (runs[left].first < runs[right].first) {
								std::swap(runs[pos], runs[left]);
								pos = left;
							} else {
								std::swap(runs[pos], runs[right]);
								pos = right;
							}
						} else {
							std::swap(runs[pos], runs[left]);
							pos = left;
						}
					} else if (runs[pos].first > runs[right].first) {
						std::swap(runs[pos], runs[right]);
						pos = right;
					} else
						break;
				} else {
					if (runs[pos].first > runs[left].first) {
						std::swap(runs[pos], runs[left]);
						pos = left;
					} else
						break;
				}
			}
		}
	}

	string new_mapto_old = Dir + "/new_mapto_old.backward.0";
	if(!OSFile::FileExists(new_mapto_old.c_str()))new_mapto_old = Dir + "/new_mapto_old.0";
	MemoryMappedFile mappedIn;
	assert(mappedIn.open(new_mapto_old.c_str()));

	const ID* reader = (const ID*) mappedIn.getBegin();
	qsort(runs, num, sizeof(VV), comp);

	cout << "Top " << num << " value as follows:" << endl;
	cout.setf(ios::fixed);
	cout.precision(3);
	for (i = num - 1; i >= 0; i--) {
		cout << reader[runs[i].second] << ": " << runs[i].first << endl;
	}

	mappedIn.close();

	for (i = 0; i < bucSize; i++) {
		free(result[i]);
		result[i] = NULL;
	}
}

void AppExample::getMaxValue(float* start, size_t size, unsigned num, VV*& runs,ID startID) {
	runs = (VV*) malloc(num * sizeof(VV));
	unsigned i = 0;
	float value;
	size_t offset = 0;
	VV temp;
	for (i = 0; i < num; i++) {
		temp.first = start[offset];
		temp.second = startID +offset;
		runs[i] = temp;
		offset++;
	}

	qsort(runs, num, sizeof(VV), comp);

	while (offset < size) {

		// Write the first entry
		value = start[offset];
		if (value <= runs[0].first) {
			offset++;
			continue;
		}

		// Update the first entry. First entry done?
		temp.first = start[offset];
		temp.second = startID+offset;
		runs[0] = temp;

		// Check the heap condition
		unsigned pos = 0, size = num;
		while (pos < size) {
			unsigned left = 2 * pos + 1, right = left + 1;
			if (left >= size)
				break;
			if (right < size) {
				if (runs[pos].first > runs[left].first) {
					if (runs[pos].first > runs[right].first) {
						if (runs[left].first < runs[right].first) {
							std::swap(runs[pos], runs[left]);
							pos = left;
						} else {
							std::swap(runs[pos], runs[right]);
							pos = right;
						}
					} else {
						std::swap(runs[pos], runs[left]);
						pos = left;
					}
				} else if (runs[pos].first > runs[right].first) {
					std::swap(runs[pos], runs[right]);
					pos = right;
				} else
					break;
			} else {
				if (runs[pos].first > runs[left].first) {
					std::swap(runs[pos], runs[left]);
					pos = left;
				} else
					break;
			}
		}
		offset++;
	}
}

Status AppExample::spmv(int preID, double* result) {
#ifdef TASK_ANALYSIS
	struct timeval start_time, end_time;
	gettimeofday(&start_time, NULL);
#endif
	const uchar *reader, *limit, *base;
	ID x = 0;
	BucketManager* bucManager1 = matrixMap[1]->getBucketManager(preID);
	reader = bucManager1->getStartPtr();
	limit = bucManager1->getEndPtr();
	base = reader - sizeof(BucketManagerMeta);

	EntityIDBuffer* ent = new EntityIDBuffer();
	ent->setIDCount(1);
	double sum = 0;
	ID* p;
	int size = 0, entsize = 0;
	int len, idoff, edgeoff, lastedgeoff;

	while (reader < limit) {
		BucketManager::getTwo(reader, size);
		if (size == 0) {
			idoff = 2;
			x = *(ID*) (reader + idoff);
			idoff += sizeof(ID);
			len = *(unsigned*) (reader + idoff);
			idoff += 4;
			if (len != 0)BucketManager::readBody(reader + idoff, len, ent);
			entsize = ent->getSize();
			p = ent->getBuffer();
			sum = 0;
			for(int i = 0; i< entsize; i++){
				sum += 1.0*p[i];
			}
			result[x] = sum;

			base += (size_t) ceil((double) (len + 10) / BucketManager::pagesize) * BucketManager::pagesize;
		} else {
			idoff = 2;
			lastedgeoff = 2 + size * (2 + sizeof(ID));
			for (int i = 0; i < size; i++) {
				x = *(ID*) (reader + idoff);
				if (x == 0)
					break;
				idoff += sizeof(ID);
				BucketManager::getTwo(reader + idoff, edgeoff);
				idoff += 2;
				if (edgeoff + 1 <= lastedgeoff) {
					cout << edgeoff + 1 << "  " << lastedgeoff << endl;
					exit(-1);
				}
				BucketManager::readBody(reader + lastedgeoff,edgeoff + 1 - lastedgeoff, ent);

				lastedgeoff = edgeoff + 1;
				entsize = ent->getSize();
				p = ent->getBuffer();
				sum = 0;
				for (int i = 0; i < entsize; i++) {
					sum += 1.0*p[i];
				}
				result[x] = sum;

			}
			base += BucketManager::pagesize;
		}
		reader = base;
	}

	delete ent;
#ifdef TASK_ANALYSIS
	gettimeofday(&end_time, NULL);
	pthread_mutex_lock(&print_mutex);
	cout<<pthread_self()<<" "<<((end_time.tv_sec - start_time.tv_sec) * 1000000 + (end_time.tv_usec - start_time.tv_usec)) / 1000.0<<" ms"<<endl;
	pthread_mutex_unlock(&print_mutex);
#endif
	return OK;
}

Status AppExample::bfs_forest(ID rootID, bool *&isVisited){
	if(isVisited[rootID])return OK;
#ifdef TASK_ANALYSIS
	struct timeval start_time, end_time;
	gettimeofday(&start_time, NULL);
#endif
	deque<ID>* que = new deque<ID>();
	EntityIDBuffer* ent = new EntityIDBuffer();
	ent->setIDCount(1);
	isVisited[rootID] = true;
	que->push_back(rootID);
	ID curID;

	while(!que->empty()){
		curID = que->front();
		que->pop_front();

		matrixMap[1]->getAllY(curID, ent, false);
		size_t entsize = ent->getSize();
		ID *p = ent->getBuffer();
		for (size_t i = 0; i < entsize; i++) {
			if(isVisited[p[i]])continue;
			isVisited[p[i]] = true;
			que->push_back(p[i]);
		}
	}

	delete ent;
	delete que;
#ifdef TASK_ANALYSIS
	gettimeofday(&end_time, NULL);
	thread_info[pthread_self()] += ((end_time.tv_sec - start_time.tv_sec) * 1000000 + (end_time.tv_usec - start_time.tv_usec));
#endif
	return OK;
}

Status AppExample::wcc(unsigned rootID, bool *&isVisited, ID *&idValue, ID *&changes){
#ifdef TASK_ANALYSIS
	struct timeval start_time, end_time;
	gettimeofday(&start_time, NULL);
#endif
	if(isVisited[rootID]){
		if(changes[rootID] < changes[idValue[rootID]]){
			ID tmp1, tmp2;
			tmp1 = changes[idValue[rootID]];
			changes[idValue[rootID]] = changes[rootID];
			while(changes[tmp1] > changes[rootID]){
				tmp2 = changes[tmp1];
				changes[tmp1] = changes[rootID];
				tmp1 = tmp2;
			}
		}
		else {
			ID tmp1, tmp2;
			tmp1 = changes[rootID];
			changes[rootID] = changes[idValue[rootID]];
			while(changes[tmp1] > changes[idValue[rootID]]){
				tmp2 = changes[tmp1];
				changes[tmp1] = changes[idValue[rootID]];
				tmp1 = tmp2;
			}
		}
#ifdef TASK_ANALYSIS
		gettimeofday(&end_time, NULL);
		thread_info[pthread_self()] += ((end_time.tv_sec - start_time.tv_sec) * 1000000 + (end_time.tv_usec - start_time.tv_usec));
#endif
		return OK;
	}

	deque<ID>* que = new deque<ID>();
	EntityIDBuffer* ent = new EntityIDBuffer();
	ent->setIDCount(1);
	isVisited[rootID] = true;
	que->push_back(rootID);
	ID component = rootID, curID;
	idValue[rootID] = component;

	while(!que->empty()){
		curID = que->front();
		que->pop_front();

		matrixMap[1]->getAllY(curID, ent, false);
		size_t entsize = ent->getSize();
		ID *p = ent->getBuffer();
		for (size_t i = 0; i < entsize; i++) {
			if(isVisited[p[i]]){
				if(component < changes[idValue[p[i]]]){
					ID tmp1, tmp2;
					tmp1 = changes[idValue[p[i]]];
					changes[idValue[p[i]]] = component;
					while(changes[tmp1] > component){
						tmp2 = changes[tmp1];
						changes[tmp1] = component;
						tmp1 = tmp2;
					}
				}
				else changes[component] = changes[idValue[p[i]]];
				continue;
			}
			
			isVisited[p[i]] = true;
			que->push_back(p[i]);
			idValue[p[i]] = component;
		}
	}

	delete ent;
	delete que;
#ifdef TASK_ANALYSIS
	gettimeofday(&end_time, NULL);
	thread_info[pthread_self()] += ((end_time.tv_sec - start_time.tv_sec) * 1000000 + (end_time.tv_usec - start_time.tv_usec));
#endif
	return OK;
}

Status AppExample::wcc_apply_changes(ID *&idValue, ID *changes, size_t size, bool isParallel) {
	if(!isParallel){
		for(size_t i = 0;i < size;i++){
			while(idValue[i] > changes[idValue[i]]){
				idValue[i] = changes[idValue[i]];
			}
		}
	}else{
		int task_num = size / MEMCPY_SIZE, remain = size % MEMCPY_SIZE;
		size_t begin = 0;

		for(int i = 0;i < task_num;i++){
			CThreadPool::getInstance().AddTask(boost::bind(&AppExample::wcc_apply_changes_task, this,
						idValue, changes, begin, begin+MEMCPY_SIZE));
			begin += MEMCPY_SIZE;
		}
		if(remain){
			CThreadPool::getInstance().AddTask(boost::bind(&AppExample::wcc_apply_changes_task, this,
					idValue, changes, begin, size));
		}
		CThreadPool::getInstance().Wait();
	}

	return OK;
}

void AppExample::wcc_apply_changes_task(ID *&idValue, ID *changes, size_t begin, size_t end) {
	for(;begin < end;begin++){
		while(idValue[begin] > changes[idValue[begin]]){
			idValue[begin] = changes[idValue[begin]];
		}
	}
}

//------------------------------work stealing application--------------------------------------

Status AppExample::pagerank(unsigned itera) {
	if (itera >= 1) {
		for (unsigned i = 0; i < bucSize; i++) {
			BucketManager* buc = matrixMap[1]->getBucketManager(i);
			scheduler->add_task_by_main_thread(boost::bind(&AppExample::pagerank_task,this,0,buc->getIndex()->getTableSize(),i, itera), i);
		}
		scheduler->set_lock_free();
		scheduler->waitForComplete();
	}

	return OK;
}

return_value* AppExample::pagerank_task(size_t startNum, size_t endNum, unsigned index, unsigned itera) {
	if (startNum >= endNum)return NULL;
#ifdef TASK_ANALYSIS
	pthread_mutex_lock(&print_mutex);
	cout<<pthread_self()<<" "<<index<<" "<<startNum<<" "<<endNum<<endl;
	pthread_mutex_unlock(&print_mutex);
	struct timeval start_time, end_time;
	gettimeofday(&start_time, NULL);
#endif
	float sum = 0;
	ID x;
	int size = 0;
	int len, idoff, edgeoff, lastedgeoff;
	BucketManager* buc = matrixMap[1]->getBucketManager(index);
	Point* offTable = buc->getIndex()->getidOffsetTable();
	const uchar* begin = buc->getStartPtr();
	const uchar* reader = begin + offTable[startNum].y;
	const uchar* base = reader;
	const uchar* limit;

	if (startNum == 0)base = reader - sizeof(BucketManagerMeta);
	if (startNum + maxRunChunk >= buc->getIndex()->getTableSize()) {
		limit = buc->getEndPtr();
		startNum = endNum;
	} else if (startNum + maxRunChunk >= endNum) {
		limit = begin + offTable[endNum].y;
		startNum = endNum;
	} else {
		limit = begin + offTable[startNum + maxRunChunk].y;
		startNum += maxRunChunk;
	}

	while (reader < limit) {
		BucketManager::getTwo(reader, size);
		if (size == 0) {
			idoff = 2;
			x = *(ID*) (reader + idoff);
			if(x == 0)break;
			idoff += sizeof(ID);
			len = *(unsigned*) (reader + idoff);
			idoff += 4;
			reader += idoff;
			const uchar* end = reader + len;
			sum = 0;
			ID y0 = 0, yi = 0;

			//read the first y (i.e., y0)
			y0 = BucketManager::readYi(reader, y0, end);
			if(itera == 1){
				assert(degree[y0].outdeg);
				sum += 1.0 / degree[y0].outdeg;
				yi = y0;
				//read yi
				while(reader < end){
					//yi = BucketManager::readYi(reader, y0, end);
					yi = BucketManager::readYi(reader, yi, end);
					assert(degree[yi].outdeg);
					sum += 1.0 / degree[yi].outdeg;
				}
			}else{
				sum += idValueIn[y0] / degree[y0].outdeg;
				yi = y0;
				//read yi
				while(reader < end){
					//yi = BucketManager::readYi(reader, y0, end);
					yi = BucketManager::readYi(reader, yi, end);
					sum += idValueIn[yi] / degree[yi].outdeg;
				}
			}

			idValueOut[x] = RANDOMRESETPROB + (1 - RANDOMRESETPROB) * sum;
			base += (size_t) ceil((double) (len + 10) / BucketManager::pagesize) * BucketManager::pagesize;
		} else {
			idoff = 2;
			lastedgeoff = 2 + size * (2 + sizeof(ID));
			const uchar* tempReader = reader;
			reader += lastedgeoff;
			for (int i = 0; i < size; i++) {
				x = *(ID*) (tempReader + idoff);
				idoff += sizeof(ID);
				BucketManager::getTwo(tempReader + idoff, edgeoff);
				idoff += 2;
				if (edgeoff + 1 <= lastedgeoff) {
					cout << edgeoff + 1 << "  " << lastedgeoff << endl;
					exit(-1);
				}

				const uchar* end = reader + (edgeoff + 1 - lastedgeoff);
				sum = 0;
				ID y0 = 0, yi = 0;

				//read the first y (i.e., y0)
				y0 = BucketManager::readYi(reader, y0, end);
				if(itera == 1){
					assert(degree[y0].outdeg);
					sum += 1.0 / degree[y0].outdeg;
					yi = y0;
					//read yi
					while(reader < end){
						//yi = BucketManager::readYi(reader, y0, end);
						yi = BucketManager::readYi(reader, yi, end);
						assert(degree[yi].outdeg);
						sum += 1.0 / degree[yi].outdeg;
					}
				}else{
					sum += idValueIn[y0] / degree[y0].outdeg;
					yi = y0;
					//read yi
					while(reader < end){
						//yi = BucketManager::readYi(reader, y0, end);
						yi = BucketManager::readYi(reader, yi, end);
						sum += idValueIn[yi] / degree[yi].outdeg;
					}
				}

				idValueOut[x] = RANDOMRESETPROB + (1 - RANDOMRESETPROB) * sum;
				lastedgeoff = edgeoff + 1;
			}
			base += BucketManager::pagesize;
		}
		reader = base;
	}

	return_value* rv = new return_value();
	rv->startChunk = startNum;
	rv->endChunk = endNum;
	rv->index = index;
	rv->generator = this;
	rv->result = NULL; //not used
	rv->itera = itera;
#ifdef TASK_ANALYSIS
	gettimeofday(&end_time, NULL);
	pthread_mutex_lock(&print_mutex);
	cout<<pthread_self()<<" "<<((end_time.tv_sec - start_time.tv_sec) * 1000000 + (end_time.tv_usec - start_time.tv_usec)) / 1000.0<<" ms"<<endl;
	pthread_mutex_unlock(&print_mutex);
#endif
	return rv;
}

Status AppExample::spmv(double *result){
	for (unsigned i = 0; i < bucSize; i++) {
		BucketManager* buc = matrixMap[1]->getBucketManager(i);
		scheduler->add_task_by_main_thread(boost::bind(&AppExample::spmv_task,this,0,buc->getIndex()->getTableSize(),i,result), i);
	}
	scheduler->set_lock_free();
	scheduler->waitForComplete();

	return OK;
}

return_value* AppExample::spmv_task(size_t startNum, size_t endNum, unsigned index, double* result){
	if (__builtin_expect(startNum >= endNum, 0))return NULL;
#ifdef TASK_ANALYSIS
	pthread_mutex_lock(&print_mutex);
	cout<<pthread_self()<<" "<<index<<" "<<startNum<<" "<<endNum<<endl;
	pthread_mutex_unlock(&print_mutex);
	struct timeval start_time, end_time;
	gettimeofday(&start_time, NULL);
#endif
	double sum = 0;
	ID x;
	int size = 0;
	int len, idoff, edgeoff, lastedgeoff;
	BucketManager* buc = matrixMap[1]->getBucketManager(index);
	Point* offTable = buc->getIndex()->getidOffsetTable();
	const uchar* begin = buc->getStartPtr();
	const uchar* reader = begin + offTable[startNum].y;
	const uchar* base = reader;
	const uchar* limit;

	if (__builtin_expect(startNum == 0, 0))base = reader - sizeof(BucketManagerMeta);
	if (startNum + maxRunChunk >= buc->getIndex()->getTableSize()) {
		limit = buc->getEndPtr();
		startNum = endNum;
	} else if (startNum + maxRunChunk >= endNum) {
		limit = begin + offTable[endNum].y;
		startNum = endNum;
	} else {
		limit = begin + offTable[startNum + maxRunChunk].y;
		startNum += maxRunChunk;
	}

	while (reader < limit) {
		BucketManager::getTwo(reader, size);
		if (__builtin_expect(size == 0, 0)) {
			idoff = 2;
			x = *(ID*) (reader + idoff);
			if(__builtin_expect(x == 0, 0))break;
			idoff += sizeof(ID);
			len = *(unsigned*) (reader + idoff);
			idoff += 4;
			reader += idoff;
			const uchar* end = reader + len;
			sum = 0;
			ID y0 = 0, yi = 0;

			//read the first y (i.e., y0)
			y0 = BucketManager::readYi(reader, y0, end);
			sum += 1.0 * y0;

			//read yi
			yi = y0;
			while(reader < end){
				//yi = BucketManager::readYi(reader, y0, end);
				yi = BucketManager::readYi(reader, yi, end);
				sum += 1.0 * yi;
			}
			result[x] = sum;

			base += (size_t) ceil((double) (len + 10) / BucketManager::pagesize) * BucketManager::pagesize;
		} else {
			idoff = 2;
			lastedgeoff = 2 + size * (2 + sizeof(ID));
			const uchar* tempReader = reader;
			reader += lastedgeoff;
			for (int i = 0; i < size; i++) {
				x = *(ID*) (tempReader + idoff);
				idoff += sizeof(ID);
				BucketManager::getTwo(tempReader + idoff, edgeoff);
				idoff += 2;
				if (__builtin_expect(edgeoff + 1 <= lastedgeoff, 0)) {
					cout << edgeoff + 1 << "  " << lastedgeoff << endl;
					exit(-1);
				}

				const uchar* end = reader + (edgeoff + 1 - lastedgeoff);
				sum = 0;
				ID y0 = 0, yi = 0;

				//read the first y (i.e., y0)
				y0 = BucketManager::readYi(reader, y0, end);
				sum += 1.0 * y0;

				//read yi
				yi = y0;
				while(reader < end){
					//yi = BucketManager::readYi(reader, y0, end);
					yi = BucketManager::readYi(reader, yi, end);
					sum += 1.0 * yi;
				}
				result[x] = sum;

				lastedgeoff = edgeoff + 1;
			}
			base += BucketManager::pagesize;
		}
		reader = base;
	}

	return_value* rv = new return_value();
	rv->startChunk = startNum;
	rv->endChunk = endNum;
	rv->index = index;
	rv->generator = this;
	rv->result = result;
	rv->itera = 0; //not used
#ifdef TASK_ANALYSIS
	gettimeofday(&end_time, NULL);
	pthread_mutex_lock(&print_mutex);
	cout<<pthread_self()<<" "<<((end_time.tv_sec - start_time.tv_sec) * 1000000 + (end_time.tv_usec - start_time.tv_usec)) / 1000.0<<" ms"<<endl;
	pthread_mutex_unlock(&print_mutex);
#endif
	return rv;
}

return_value* AppExample::pagerank_or_spmv_task(size_t startNum, size_t endNum, unsigned index, unsigned itera, double* result){
	if(this->is_pagerank_not_spmv){
		return pagerank_task(startNum, endNum, index, itera);
	}
	else{
		return spmv_task(startNum, endNum, index, result);
	}

}

Status AppExample::bfs_forest(unsigned max_index, ID *root_arr, bool *&isVisited){
	this->root_array = root_arr;
	this->isVisited = isVisited;

	for (unsigned i = 0; i < SPECIAL_ROOT_SIZE; i++) {
		scheduler->add_task_by_main_thread(boost::bind(&AppExample::bfs_forest_task,this,i,i+1,max_index), i);
	}
	scheduler->set_lock_free();
	scheduler->waitForComplete();

	return OK;
}

return_value* AppExample::bfs_forest_task(unsigned begin, unsigned end, unsigned max_index){
#ifdef TASK_ANALYSIS
	struct timeval start_time, end_time;
	gettimeofday(&start_time, NULL);
#endif
	deque<ID>* que = new deque<ID>();
	EntityIDBuffer* ent = new EntityIDBuffer();
	ent->setIDCount(1);
	ID curID;

	for(;begin < end;begin++){
		if(isVisited[root_array[begin]])continue;
		isVisited[root_array[begin]] = true;
		que->push_back(root_array[begin]);

		while(!que->empty()){
			curID = que->front();
			que->pop_front();

			matrixMap[1]->getAllY(curID, ent, false);
			size_t entsize = ent->getSize();
			ID *p = ent->getBuffer();
			for (size_t i = 0; i < entsize; i++) {
				if(isVisited[p[i]])continue;
				isVisited[p[i]] = true;
				que->push_back(p[i]);
			}
		}
	}

	delete ent;
	delete que;

	return_value* rv = new return_value();
	rv->index = max_index;
	rv->generator = this;
	rv->startChunk = 0; //not used
	rv->endChunk = 0;
	rv->result = NULL;
	rv->itera = 0;
#ifdef TASK_ANALYSIS
	gettimeofday(&end_time, NULL);
	thread_info[pthread_self()] += ((end_time.tv_sec - start_time.tv_sec) * 1000000 + (end_time.tv_usec - start_time.tv_usec));
#endif
	return rv;
}

Status AppExample::wcc(unsigned max_index, ID *root_arr, bool *&isVisited, ID *&idValue, ID *&changes){
	this->root_array = root_arr;
	this->isVisited = isVisited;
	this->idValue = idValue;
	this->changes = changes;

	for (unsigned i = 0; i < SPECIAL_ROOT_SIZE; i++) {
		scheduler->add_task_by_main_thread(boost::bind(&AppExample::wcc_task,this,i,i+1,max_index), i);
	}
	scheduler->set_lock_free();
	scheduler->waitForComplete();

	return OK;
}

return_value* AppExample::wcc_task(unsigned begin, unsigned end, unsigned max_index){
#ifdef TASK_ANALYSIS
	struct timeval start_time, end_time;
	gettimeofday(&start_time, NULL);
#endif
	deque<ID>* que = new deque<ID>();
	EntityIDBuffer* ent = new EntityIDBuffer();
	ent->setIDCount(1);
	ID curID, component;

	for(;begin < end;begin++){
		if(isVisited[root_array[begin]]){
			if(changes[root_array[begin]] < changes[idValue[root_array[begin]]]){
				ID tmp1, tmp2;
				tmp1 = changes[idValue[root_array[begin]]];
				changes[idValue[root_array[begin]]] = changes[root_array[begin]];
				while(changes[tmp1] > changes[root_array[begin]]){
					tmp2 = changes[tmp1];
					changes[tmp1] = changes[root_array[begin]];
					tmp1 = tmp2;
				}
			}
			else{
				ID tmp1, tmp2;
				tmp1 = changes[root_array[begin]];
				changes[root_array[begin]] = changes[idValue[root_array[begin]]];
				while(changes[tmp1] > changes[idValue[root_array[begin]]]){
					tmp2 = changes[tmp1];
					changes[tmp1] = changes[idValue[root_array[begin]]];
					tmp1 = tmp2;
				}
			}
			continue;
		}
		
		isVisited[root_array[begin]] = true;
		que->push_back(root_array[begin]);
		component = root_array[begin];
		idValue[root_array[begin]] = component;

		while(!que->empty()){
			curID = que->front();
			que->pop_front();

			matrixMap[1]->getAllY(curID, ent, false);
			size_t entsize = ent->getSize();
			ID *p = ent->getBuffer();
			for (size_t i = 0; i < entsize; i++) {
				if(isVisited[p[i]]){
					if(component < changes[idValue[p[i]]]){
						ID tmp1, tmp2;
						tmp1 = changes[idValue[p[i]]];
						changes[idValue[p[i]]] = component;
						while(changes[tmp1] > component){
							tmp2 = changes[tmp1];
							changes[tmp1] = component;
							tmp1 = tmp2;
						}
					}
					else changes[component] = changes[idValue[p[i]]];
					continue;
				}
				
				isVisited[p[i]] = true;
				que->push_back(p[i]);
				idValue[p[i]] = component;
			}
		}
	}

	delete ent;
	delete que;

	return_value* rv = new return_value();
	rv->index = max_index;
	rv->generator = this;
	rv->startChunk = 0; //not used
	rv->endChunk = 0;
	rv->result = NULL;
	rv->itera = 0;
#ifdef TASK_ANALYSIS
	gettimeofday(&end_time, NULL);
	thread_info[pthread_self()] += ((end_time.tv_sec - start_time.tv_sec) * 1000000 + (end_time.tv_usec - start_time.tv_usec));
#endif
	return rv;
}

return_value* AppExample::bfs_forest_or_wcc_task(unsigned begin, unsigned end, unsigned max_index){
	if(this->is_bfs_not_wcc){
		return bfs_forest_task(begin, end, max_index);
	}else{
		return wcc_task(begin, end, max_index);
	}
}
