
#include <string>
#include <vector>
#include <map>
#include <rapidjson/reader.h> //rapidjson-dev
using namespace rapidjson;
#include <iostream>
using namespace std;

int ReadFileContents(const char *filename, int binaryMode, std::string &contentOut);

std::vector<std::string> split(const std::string &s, char delim);

// http://rapidjson.org/md_doc_sax.html
class JsonToStringMap : public BaseReaderHandler<UTF8<>, JsonToStringMap>
{
protected:
	std::string currentKey;
public:
	map<std::string, std::string> tagMap;

    bool Null() { //cout << "Null()" << endl; 
		return true; }
    bool Bool(bool b) { //cout << "Bool(" << boolalpha << b << ")" << endl; 
		return true; }
    bool Int(int i) { //cout << "Int(" << i << ")" << endl; 
		return true; }
    bool Uint(unsigned u) { //cout << "Uint(" << u << ")" << endl; 
		return true; }
    bool Int64(int64_t i) { //cout << "Int64(" << i << ")" << endl; 
		return true; }
    bool Uint64(uint64_t u) { //cout << "Uint64(" << u << ")" << endl; 
		return true; }
    bool Double(double d) { //cout << "Double(" << d << ")" << endl; 
		return true; }
    bool String(const char* str, SizeType length, bool copy) { 
        //cout << "String(" << str << ", " << length << ", " << boolalpha << copy << ")" << endl;
		tagMap[currentKey] = str;
        return true;
    }
    bool StartObject() { 
		//cout << "StartObject()" << endl; 
		tagMap.clear();
		return true; 
	}
    bool Key(const char* str, SizeType length, bool copy) { 
        //cout << "Key(" << str << ", " << length << ", " << boolalpha << copy << ")" << endl;
		currentKey = str;
        return true;
    }
    bool EndObject(SizeType memberCount) { 
		//cout << "EndObject(" << memberCount << ")" << endl; 
		return true; }
    bool StartArray() { 
		//cout << "StartArray()" << endl; 
		return true; }
    bool EndArray(SizeType elementCount) { 
		//cout << "EndArray(" << elementCount << ")" << endl; 
		return true; }
};

