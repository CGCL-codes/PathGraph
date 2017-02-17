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

#include "URITable.h"
#include "StringIDSegment.h"
#include <string.h>

ID URITable::startID = 1;

ID URITable::getMaxID()
{
	return suffix_segment->getMaxID();
}

URITable::URITable() {
	SINGLE.assign("single");
	prefix_segment = NULL;
	suffix_segment = NULL;
}

URITable::URITable(const string dir) : SINGLE("single") {
	// TODO Auto-generated constructor stub
	prefix_segment = StringIDSegment::create(dir, "uri_prefix");
	suffix_segment = StringIDSegment::create(dir, "uri_suffix");

	prefix_segment->addStringToSegment(SINGLE);
}

URITable::~URITable() {
	// TODO Auto-generated destructor stub
#ifdef DEBUG
	cout<<"destroy URITable"<<endl;
#endif
	if(prefix_segment != NULL)
		delete prefix_segment;
	prefix_segment = NULL;

	if(suffix_segment != NULL)
		delete suffix_segment;
	suffix_segment = NULL;
}

Status URITable::getIdByURI(const char* URI,ID& id)
{
	getPrefix(URI);
	if (prefix.equals(SINGLE.c_str())) {
		searchStr.clear();
		searchStr.insert(searchStr.begin(), 2);
		searchStr.append(suffix.str, suffix.length);
		searchLen.str = searchStr.c_str();
		searchLen.length = searchStr.length();
		if (suffix_segment->findIdByString(id, &searchLen) == false)
			return URI_NOT_FOUND;
	} else {
		char temp[10];
		ID prefixId;
		if (prefix_segment->findIdByString(prefixId, &prefix) == false) {
			return URI_NOT_FOUND;
		} else {
			sprintf(temp, "%d", prefixId);
			searchStr.assign(suffix.str, suffix.length);
			for (size_t i = 0; i < strlen(temp); i++) {
#ifdef USE_C_STRING
				searchStr.insert(searchStr.begin() + i, temp[i] - '0' + 1);
#else
				searchStr.insert(searchStr.begin() + i, temp[i] - '0');
#endif
			}

			searchLen.str = searchStr.c_str();
			searchLen.length = searchStr.length();
			if (suffix_segment->findIdByString(id, &searchLen) == false)
				return URI_NOT_FOUND;
		}
	}

	searchStr.clear();
	return URI_FOUND;
}

Status URITable::getPrefix(const char* URI)
{
	size_t size = strlen(URI);
	int i;
	for (i = size - 2; i >= 0; i--) {
		if (URI[i] == '/')
			break;
	}

	if (i == -1) {
		prefix.str = SINGLE.c_str();
		prefix.length = SINGLE.length();
		suffix.str = URI;
		suffix.length = size;
	} else {
		prefix.str = URI;
		prefix.length = i;
		suffix.str = URI + i + 1;
		suffix.length = size - i - 1;
	}

	return OK;
}

Status URITable::insertTable(const char* URI, ID& id)
{
	getPrefix(URI);
	char temp[20];
	ID prefixId;

	prefixId = 1;
	if(prefix_segment->findIdByString(prefixId, &prefix) == false)
		prefixId = prefix_segment->addStringToSegment(&prefix);
	sprintf(temp, "%d",prefixId);

	searchStr.assign(suffix.str, suffix.length);
	for(size_t i = 0; i < strlen(temp); i++) {
#ifdef USE_C_STRING
		searchStr.insert(searchStr.begin() + i, temp[i] - '0' + 1);//suffix.insert(suffix.begin() + i, temp[i] - '0');
#else
		searchStr.insert(searchStr.begin() + i, temp[i] - '0');
#endif
	}

	searchLen.str = searchStr.c_str(); searchLen.length = searchStr.length();
	id = suffix_segment->addStringToSegment(&searchLen);
	searchStr.clear();
	return OK;
}

Status URITable::getURIById(string& URI, ID id)
{
	URI.clear();
	if (suffix_segment->findStringById(&suffix, id) == false)
		return URI_NOT_FOUND;
	char temp[10];
	memset(temp, 0, 10);
	const char* ptr = suffix.str;

	int i;
#ifdef USE_C_STRING
	for (i = 0; i < 10; i++) {
		if (ptr[i] > 10)
			break;
		temp[i] = (ptr[i] - 1) + '0';
	}
#else
	for(i = 0; i < 10; i++) {
		if(ptr[i] > 9)
			break;
		temp[i] = ptr[i] + '0';
	}
#endif

	ID prefixId = atoi(temp);
	if (prefixId == 1)
		URI.assign(suffix.str + 1, suffix.length - 1);
	else {
		if (prefix_segment->findStringById(&prefix, prefixId) == false)
			return URI_NOT_FOUND;
		URI.assign(prefix.str, prefix.length);
		URI.append("/");
		URI.append(suffix.str + i, suffix.length - i);
	}

	return OK;
}

URITable* URITable::load(const string dir)
{
	URITable* uriTable = new URITable();
	uriTable->prefix_segment = StringIDSegment::load(dir, "uri_prefix");
	uriTable->suffix_segment = StringIDSegment::load(dir, "uri_suffix");
	return uriTable;
}

void URITable::dump()
{
	prefix_segment->dump();
	suffix_segment->dump();
}
