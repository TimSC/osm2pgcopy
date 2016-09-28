//g++ dump.cpp util.cpp cppo5m/o5m.cpp cppo5m/varint.cpp -Wall -lpqxx -o dump
#include "cppo5m/o5m.h"
#include <pqxx/pqxx> //apt install libpqxx-dev
#include <fstream>
#include <iostream>
#include <sstream>
#include "util.h"
using namespace std;

int main(int argc, char **argv)
{	
	std::filebuf outfi;
	outfi.open("o5mtest2.o5m", std::ios::out);
	O5mEncode enc(outfi);
	enc.StoreIsDiff(false);

	string configContent;
	cout << "Reading settings from config.cfg" << endl;
	ReadFileContents("config.cfg", false, configContent);
	std::vector<std::string> lines = split(configContent, '\n');
	std::map<string, string> config;
	for(size_t i=0; i < lines.size(); i++)
	{
		const std::string &line = lines[i];
		std::vector<std::string> parts = split(line, ':');
		if (parts.size() < 2) continue;
		config[parts[0]] = parts[1];
	}
	
	std::stringstream ss;
	ss << "dbname=";
	ss << config["dbname"];
	ss << " user=";
	ss << config["dbuser"];
	ss << " password='";
	ss << config["dbpass"];
	ss << "' hostaddr=";
	ss << config["dbhost"];
	ss << " port=";
	ss << "5432";
	
	pqxx::connection dbconn(ss.str());
	if (dbconn.is_open()) {
		cout << "Opened database successfully: " << dbconn.dbname() << endl;
	} else {
		cout << "Can't open database" << endl;
		return 1;
	}
	
	//Dump nodes
	stringstream sql;
	sql << "SELECT *, ST_X(geom) as lon, ST_Y(geom) AS lat FROM ";
	sql << config["dbtableprefix"];
	sql << "nodes WHERE visible=true and current=true;";

	pqxx::work work(dbconn);
	pqxx::icursorstream cursor( work, sql.str(), "nodecursor", 1000 );	
	uint64_t count = 0;
	for ( size_t idx = 0; true; idx ++ )
	{
		pqxx::result rows;
		cursor.get(rows);
		if ( rows.empty() ) break; // nothing left to read

		for (pqxx::result::const_iterator c = rows.begin(); c != rows.end(); ++c) {
			//cout << "ID = " << c[0].as<int>() << endl;
			count ++;
			if(count % 1000000 == 0)
				cout << count << " nodes" << endl;
		}
	}

	enc.Finish();
	outfi.close();
	dbconn.disconnect ();
	return 0;
}
