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

#include "MatrixMapOp.h"
#include "list"

MatrixMapOp::~MatrixMapOp() {
	// TODO Auto-generated destructor stub
}

MatrixMapOp::MatrixMapOp() {
	// TODO Auto-generated constructor stub

}

Status MatrixMapOp::matrixFactorization(char n,map<char,Node>& treeIndex){
	// giving a method to do exponent decomposition
	list<char> cache;
	if(n < 2)
		return ERR;
	Node node;
	Node node0;
	node0.left = node0.right = 0;

	cache.push_back(n);
	do{
		if(cache.front() < 4){
			treeIndex[cache.front()] = node0;
			cache.pop_front();
			continue;
		}
		if(cache.front() % 2 == 0){
			node.left = cache.front()/2;
			node.right = 0;
			cache.push_back(node.left);
		}else{
			node.left = cache.front()/2;
			node.right = cache.front() - node.left;
			cache.push_back(node.right);
			cache.push_back(node.left);
		}
		treeIndex[cache.front()] = node;
		cache.pop_front();
	}while(cache.size() > 0);
	return OK;
}

Status MatrixMapOp::matrixMultiply(Matrixmap* matrix1, Matrixmap* matrix2, char* upperDir,ID preID){
	//offen matrix1 is bigger than matrix2
	cout << "MatrixMultiply preID:" << preID << endl;

	BucketManager* bucManager = matrix1->getPredicate_manager()[preID];
	if(bucManager == NULL || bucManager->getLineCount() ==0)
		return OK;
	int degree = matrix1->getDegree() + matrix2->getDegree();
	char name[10];
	sprintf(name,"Matrix%d",degree);
	if(!matrixMap.count(degree))
		matrixMap[degree] = new Matrixmap(upperDir, name);

	const uchar* base = bucManager->getStartPtr()- sizeof(BucketManagerMeta);
	const uchar* reader = bucManager->getStartPtr();
	const uchar* limit = bucManager->getEndPtr();
	ID x = 0;
	unsigned len= 0,chunkleft = MemoryBuffer::pagesize - sizeof(BucketManagerMeta);
	unsigned tableCount = 0;
	EntityIDBuffer* entBuffer = new EntityIDBuffer();
	EntityIDBuffer* curBuffer = new EntityIDBuffer();

	while(reader < limit){
		if(chunkleft <=4){	//step into the chunk end with a little space
			tableCount = (int) ::ceil((double) (reader - base)/ MemoryBuffer::pagesize);
			reader = base + tableCount * MemoryBuffer::pagesize;
			chunkleft = MemoryBuffer::pagesize;
			continue;
		}
		BucketManager::readHead(reader,x,len);
		if(reader)
		if(x == 0){			//step into the chunk end with a lot of 0
			tableCount = (int)::ceil((double)(reader-base)/MemoryBuffer::pagesize);
			reader = base + tableCount* MemoryBuffer::pagesize;
			chunkleft = MemoryBuffer::pagesize;
			continue;
		}

		//printf("%d  %u  %u  %d\n",x,reader-base,limit-base,len);
		BucketManager::readChars(reader,x,entBuffer);

		size_t total = entBuffer->getSize();
		ID* p = entBuffer->getBuffer();

		matrix2->getAllYByID(p[0],curBuffer,preID);
		for (size_t i = 1; i != total; i++) {
			matrix2->getAllYByID(p[i],curBuffer,preID);
			if(curBuffer->getSize() > 0)
				matrixMap[degree]->getBucketManager(preID)->insertXY(x,curBuffer);
		}

		reader += len;
		chunkleft -= len;
	}
	matrix2->clearCache(preID);
	matrixMap[degree]->endInsertXY(preID);

	delete entBuffer;
	delete curBuffer;
	return OK;
}
