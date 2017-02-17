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

#ifndef CYCLECACHE_H_
#define CYCLECACHE_H_

#include "Matrix.h"
#include "TripleBit.h"
//#include "MutexLock.h"
//template <class K,class V>
typedef ID K;
typedef EntityIDBuffer* V;

class CycleCache {
public:
	map<K, V> mapCache;
	unsigned cacheSize;
	unsigned dropSize;
	vector<K> keyList;

//	static MutexLock lock;
	CycleCache() {
		this->cacheSize = TripleBit_Find_Cache;
		this->dropSize = TripleBit_Drop_Cache;
		keyList.reserve(cacheSize);
//		lock.init();
	}
	CycleCache(unsigned cacheSize,unsigned dropSize) {
		this->cacheSize = cacheSize;
		this->dropSize = dropSize;
		keyList.reserve(cacheSize);
	}
	virtual ~CycleCache() {
		keyList.clear();
		map<K, V>::iterator iter = mapCache.begin();
		for(;iter != mapCache.end();iter++){
			delete iter->second;
			iter->second = NULL;
		}
		mapCache.clear();
	}

	void clear(){
		keyList.clear();
		map<K, V>::iterator iter = mapCache.begin();
		for (; iter != mapCache.end(); iter++) {
			delete iter->second;
		}
		mapCache.clear();
	}

	unsigned getSize() {
		return mapCache.size();
	}
	Status insert(K key, V value) {
	//	lock.lock();
		if (getSize() >= cacheSize) {
			K delKey = *keyList.begin();
			for (unsigned i = 0; i < dropSize; i++) {
				delKey = *(keyList.begin() + i);
				delete mapCache[delKey];
				mapCache.erase(delKey);
			}
			keyList.erase(keyList.begin(), keyList.begin()
					+ dropSize);
		}
		//value->print();
		if(mapCache.insert(make_pair(key,value)).second)
			keyList.push_back(key);
	//	lock.unLock();
		return OK;
	}
	Status getValuebyKey(K key, V& value) {
		if (mapCache.count(key)) {
			if(mapCache[key] == NULL){
				value->empty();
				return OK;
			}
			//value =mapCache[key];
			value->operator =(mapCache[key]);
//			printf("read value %u %x %u\n",key,value,*value->buffer);
			return OK;
		} else
			return NOT_FOUND;
	}


	/*Status testintprint(){
		map<int,int>::iterator it = mapCache.begin();
		cout << "size:" << getSize() << endl;
		while(it != mapCache.end()){
			cout << it->first << "   " << it->second << endl;
			it++;
		}
		cout << "======================================="<< endl;
	}*/
};
#endif /* CYCLECACHE_H_ */
