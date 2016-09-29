//g++ dump.cpp util.cpp cppo5m/o5m.cpp cppo5m/varint.cpp -Wall -lpqxx -o dump
#include "cppo5m/o5m.h"
#include <pqxx/pqxx> //apt install libpqxx-dev
#include <fstream>
#include <iostream>
#include <sstream>
#include "util.h"
#include "cppGzip/EncodeGzip.h"
#include <assert.h>
#include <time.h>
using namespace std;

int main(int argc, char **argv)
{	
	std::filebuf outfi;
	outfi.open("dump.o5m.gz", std::ios::out);
	EncodeGzip *gzipEnc = new class EncodeGzip(outfi);

	O5mEncode enc(*gzipEnc);
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
	
	cout << "Dump nodes" << endl;
	stringstream sql;
	sql << "SELECT *, ST_X(geom) as lon, ST_Y(geom) AS lat FROM ";
	sql << config["dbtableprefix"];
	sql << "nodes WHERE visible=true and current=true;";

	pqxx::work work(dbconn);
	pqxx::icursorstream cursor( work, sql.str(), "nodecursor", 1000 );	
	uint64_t count = 0;
	class MetaData metaData;
	JsonToStringMap handler;
	double lastUpdateTime = (double)clock() / CLOCKS_PER_SEC;
	uint64_t lastUpdateCount = 0;
	for ( size_t batch = 0; true; batch ++ )
	{
		pqxx::result rows;
		cursor.get(rows);
		if ( rows.empty() ) break; // nothing left to read
		if(batch == 0)
		{
			size_t numCols = rows.columns();
			for(size_t i = 0; i < numCols; i++)
			{
				cout << i << "\t" << rows.column_name(i) << "\t" << (unsigned int)rows.column_type((pqxx::tuple::size_type)i) << endl;
			}
		}

		int idCol = rows.column_number("id");
		int changesetCol = rows.column_number("changeset");
		int usernameCol = rows.column_number("username");
		int uidCol = rows.column_number("uid");
		int visibleCol = rows.column_number("visible");
		int timestampCol = rows.column_number("timestamp");
		int versionCol = rows.column_number("version");
		int currentCol = rows.column_number("current");
		int tagsCol = rows.column_number("tags");
		int latCol = rows.column_number("lat");
		int lonCol = rows.column_number("lon");

		for (pqxx::result::const_iterator c = rows.begin(); c != rows.end(); ++c) {

			bool visible = c[visibleCol].as<bool>();
			bool current = c[currentCol].as<bool>();
			assert(visible && current);

			int64_t objId = c[idCol].as<int64_t>();
			double lat = atof(c[latCol].c_str());
			double lon = atof(c[lonCol].c_str());
			metaData.version = c[versionCol].as<uint64_t>();
			metaData.timestamp = c[timestampCol].as<int64_t>();
			metaData.changeset = c[changesetCol].as<int64_t>();
			if (c[uidCol].is_null())
				metaData.uid = 0;
			else
				metaData.uid = c[uidCol].as<uint64_t>();
			if (c[usernameCol].is_null())
				metaData.username = "";
			else
				metaData.username = c[usernameCol].c_str();
			
			handler.tagMap.clear();
			string tagsJson = c[tagsCol].as<string>();
			if (tagsJson != "{}")
			{
				Reader reader;
				StringStream ss(tagsJson.c_str());
				reader.Parse(ss, handler);
			}

			count ++;
			if(count % 1000000 == 0)
				cout << count << " nodes" << endl;

			double timeNow = (double)clock() / CLOCKS_PER_SEC;
			if (timeNow - lastUpdateTime > 1.0)
			{
				cout << count - lastUpdateCount << " nodes/sec" << endl;
				lastUpdateCount = count;
				lastUpdateTime = timeNow;
			}

			enc.StoreNode(objId, metaData, handler.tagMap, lat, lon);
		}

		if (count >= 3000000)
			exit(0);
	}

	enc.Finish();
	delete gzipEnc;
	outfi.close();
	dbconn.disconnect ();
	return 0;
}
