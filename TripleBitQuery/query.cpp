//---------------------------------------------------------------------------
// TripleBit
// (c) 2011 Massive Data Management Group @ SCTS & CGCL. 
//     Web site: http://grid.hust.edu.cn/triplebit
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------

#include "TripleBitRepository.h"

int main(int argc, char* argv[])
{
	if(argc != 3) {
		fprintf(stderr, "Usage: %s <TripleBit Directory> <Query files Directory>\n", argv[0]);
		return -1;
	}


	TripleBitRepository* repo = TripleBitRepository::create(argv[1]);
	if(repo == NULL) {
		return -1;
	}

	repo->cmd_line(stdin, stdout, argv[2]);


/*	unsigned offset = 0;
	repo->getBitmapBuffer()->predicate_managers[0][4]->getChunkIndex(1)->getFirstOffsetByID(
			135, offset, 2);
	cout << "offset:" << offset << endl;*/


	delete repo;

	return 0;
}
