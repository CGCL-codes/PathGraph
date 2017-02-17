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

#ifndef MATRIXMAPOP_H_
#define MATRIXMAPOP_H_

#include "Matrixmap.h"
#include "map"
#include "MutexLock.h"
struct Node {
	char left;
	char right;
};
class MatrixMapOp {
	map<char,Matrixmap*> matrixMap;
	MutexLock lock;
public:
	virtual ~MatrixMapOp();
	MatrixMapOp();
	static Status matrixFactorization(char n,map<char,Node>&);
	Status matrixMultiply(Matrixmap* matrix1, Matrixmap* matrix2,char* upperDir,ID preID);
};

#endif /* MATRIXMAPOP_H_ */
