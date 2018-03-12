#include "gpudb/GPUdb.hpp"

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/optional/optional_io.hpp>
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>
#include <iterator>
#include <boost/tokenizer.hpp>
#include <cstdlib>

void run(gpudb::GPUdb &gpudb)
{
    using namespace std;
    using namespace boost;

    //set up for csv file parsing
    string data("fuels_inventory.csv");
    ifstream in(data.c_str());
    typedef tokenizer< escaped_list_separator<char> > Tokenizer;

    std::map<std::string, std::string> options;

    // Get the version information
    std::cout << "GPUdb C++ Client Version: " << gpudb.getApiVersion() << std::endl;

    // Create some test data

    //create columns for the table
    std::vector<gpudb::Type::Column> columns;
    columns.push_back(gpudb::Type::Column("TIMESTAMP", gpudb::Type::Column::STRING));
    columns.push_back(gpudb::Type::Column("GASOLINE_STOCK", gpudb::Type::Column::DOUBLE));
    gpudb::Type newType = gpudb::Type("Test", columns);
    std::string typeId = newType.create(gpudb);
    std::string table_name = "Test";


    try
    {
        //typeID: Type("Test", columns).create())
        //options: map<string, string>
        gpudb.createTable(table_name, typeId, options);

        std::vector<gpudb::GenericRecord> data;

        cout << "TIMESTAMP" << "\t" << "GASOLINE_STOCK" << endl;
        vector<string> vec;
        string line;

        int i = 0;
        while(getline(in, line)){
            if(i != 0){
                Tokenizer tok(line);
                vec.assign(tok.begin(), tok.end());
                gpudb::GenericRecord record(newType);
                record.stringValue("TIMESTAMP") = vec[0];
                record.doubleValue("GASOLINE_STOCK") = atof(vec[5].c_str());
                data.push_back(record);
            }
            i++;
        }

        gpudb.insertRecords(table_name, data, options);

        std::cout << "done inserting records" << std::endl;
    }
    catch (const std::exception& e)
    {
        std::cout << " caught exception: " << e.what() << " continuing" << std::endl;
    }

    // Group by value

    std::vector<std::string> gbvColumns;
    gbvColumns.push_back("TIMESTAMP");
    std::map<std::string, std::string> gbParams;
    // gbParams["sort_order"] = "descending";
    // gbParams["sort_by"] = "value";

    std::cout << "calling groupby (new)" << std::endl;

    std::vector<std::string> gbColumns;
    gbColumns.push_back("TIMESTAMP");
    gbColumns.push_back("GASOLINE_STOCK");

    //(tableName, columnNames, offset, limit, options)
    gpudb::AggregateGroupByResponse gbResponse = gpudb.aggregateGroupBy(table_name, gbColumns, 0, 10, gbParams);
    std::cout << "got groupby response, data size: " << gbResponse.data.size() << std::endl;
    
    // for (size_t i = 0; i < gbResponse.data.size(); ++i)
    // {
    //     gpudb::GenericRecord& record = gbResponse.data[i];
    //     const gpudb::Type& type = record.getType();
    //     if (i == 0)
    //     {
    //         std::cout << "Number of fields : " << type.getColumnCount() << std::endl;
    //         for (size_t j = 0; j < type.getColumnCount(); ++j)
    //         {
    //             std::cout << type.getColumn(j).getName() << "\t";
    //         }
    //         std::cout << std::endl;
    //     }

    //     for (size_t j=0;j<type.getColumnCount();++j)
    //     {
    //         std::cout << record.toString(j) << "\t";
    //     }
    //     std::cout << std::endl;
    // }

    std::string table_view_name = "TestResult";
    gpudb.filter(table_name, table_view_name, "GASOLINE_STOCK > 24000", options);

    gpudb::GetRecordsResponse<gpudb::GenericRecord> gsoResponse = gpudb.getRecords<gpudb::GenericRecord>(newType, table_view_name, 0, 100, options);

    for (size_t i = 0; i < gsoResponse.data.size(); ++i)
    {
        const gpudb::GenericRecord& record = gsoResponse.data[i];
        std::cout << record.toString("TIMESTAMP")
                << " " << record.toString("GASOLINE_STOCK")
                << std::endl;
    }
    gpudb.clearTable(table_view_name, "", options);
    gpudb.clearTable(table_name, "", options);
}

int main(int argc, char* argv[])
{
    if (argc < 2)
    {
        std::cout << "Please enter local address of your running instance: " << std::endl;

        exit(1);
    }

    std::vector<std::string> hosts;
    boost::split(hosts, argv[1], boost::is_any_of(","));

    #ifndef GPUDB_NO_HTTPS
    // Use SSL context that does no certificate validation (for example purposes only)
    boost::asio::ssl::context sslContext(boost::asio::ssl::context::sslv23);
    gpudb::GPUdb::Options opts = gpudb::GPUdb::Options().setTimeout(1000).setSslContext(&sslContext);
    #else
    gpudb::GPUdb::Options opts = gpudb::GPUdb::Options().setTimeout(1000);
    #endif

    std::string host(hosts[0]);
    std::cout << "Connecting to GPUdb host: '" << host << "'" << std::endl;
    gpudb::GPUdb gpudb(host, opts);
    run(gpudb);



    return 0;
}
