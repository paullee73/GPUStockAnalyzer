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
#include <stdio.h>
void run(gpudb::GPUdb &gpudb, std::string relationDate, std::string specificDate, std::string relationPrice, std::string specificPrice)
{
    using namespace std;
    using namespace boost;

    //set up for csv file parsing
    string data("fuels_inventory.csv");
    ifstream in(data.c_str());
    typedef tokenizer< escaped_list_separator<char> > Tokenizer;

    std::map<std::string, std::string> options;

    // Get the version information
    // std::cout << "GPUdb C++ Client Version: " << gpudb.getApiVersion() << std::endl;

    //create columns for the table
    std::vector<gpudb::Type::Column> columns;
    columns.push_back(gpudb::Type::Column("TIMESTAMP", gpudb::Type::Column::DOUBLE));
    columns.push_back(gpudb::Type::Column("GASOLINE_STOCK", gpudb::Type::Column::DOUBLE));
    columns.push_back(gpudb::Type::Column("DATE", gpudb::Type::Column::STRING));
    gpudb::Type newType = gpudb::Type("Test", columns);
    std::string typeId = newType.create(gpudb);
    std::string table_name = "Test";


    try
    {
        //typeID: Type("Test", columns).create())
        //options: map<string, string>
        gpudb.createTable(table_name, typeId, options);

        std::vector<gpudb::GenericRecord> data;

        //cout << "TIMESTAMP" << "\t" << "GASOLINE_STOCK" << endl;
        vector<string> vec;
        string line;

        int i = 0;
        while(getline(in, line)){
            if(i != 0){
                Tokenizer tok(line);
                vec.assign(tok.begin(), tok.end());
                gpudb::GenericRecord record(newType);
                record.doubleValue("TIMESTAMP") = atof(vec[0].substr(0,2).c_str())*2592000
                    + atof(vec[0].substr(3,5).c_str())*86400 + atof(vec[0].substr(6,10).c_str())*31557600;
                record.doubleValue("GASOLINE_STOCK") = atof(vec[5].c_str());
                record.stringValue("DATE") = vec[0];
                data.push_back(record);
            }
            i++;
        }

        gpudb.insertRecords(table_name, data, options);

    }
    catch (const std::exception& e)
    {
        std::cout << " caught exception: " << e.what() << " continuing" << std::endl;
    }

    // Group by value
    // std::vector<std::string> gbvColumns;
    // gbvColumns.push_back("TIMESTAMP");
    // std::map<std::string, std::string> gbParams;
    // // gbParams["sort_order"] = "descending";
    // // gbParams["sort_by"] = "value";

    // std::cout << "calling groupby (new)" << std::endl;

    // std::vector<std::string> gbColumns;
    // gbColumns.push_back("TIMESTAMP");
    // gbColumns.push_back("GASOLINE_STOCK");

    // //(tableName, columnNames, offset, limit, options)
    // gpudb::AggregateGroupByResponse gbResponse = gpudb.aggregateGroupBy(table_name, gbColumns, 0, 10, gbParams);
    // std::cout << "got groupby response, data size: " << gbResponse.data.size() << std::endl;
    
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

    //convert input date to seconds
    double specificTime;
    if(specificDate.size() != 0){
        specificTime = atof(specificDate.substr(0,2).c_str())*2592000
                    + atof(specificDate.substr(3,5).c_str())*86400 + atof(specificDate.substr(6,10).c_str())*31557600;
    }

    std::string table_view_name = "TestResult";
    std::string specificParam = "TIMESTAMP"+boost::lexical_cast<std::string>(" ")+boost::lexical_cast<std::string>(relationDate)+
        boost::lexical_cast<std::string>(" ")+boost::lexical_cast<std::string>(specificTime)+
        " and GASOLINE_STOCK "+boost::lexical_cast<std::string>(relationPrice)+
        boost::lexical_cast<std::string>(" ")+boost::lexical_cast<std::string>(specificPrice);
    gpudb.filter(table_name, table_view_name, specificParam, options);

    gpudb::GetRecordsResponse<gpudb::GenericRecord> gsoResponse = gpudb.getRecords<gpudb::GenericRecord>(newType, table_view_name, 0, 800, options);
    //freopen("output.txt", "w", stdout);
    string nextLine = "\n";
    for (size_t i = 0; i < gsoResponse.data.size(); ++i)
    {
        const gpudb::GenericRecord& record = gsoResponse.data[i];
        cout << record.toString("DATE").c_str() << " " << record.toString("GASOLINE_STOCK").c_str() << endl;
        //printf(record.toString("DATE").c_str());
        //printf("\t");
        //printf(record.toString("GASOLINE_STOCK").c_str());
        //printf(nextLine.c_str());
    }
    fclose(stdout);
    gpudb.clearTable(table_view_name, "", options);
    gpudb.clearTable(table_name, "", options);
}

int main(int argc, char* argv[])
{
    if (argc < 5)
    {
        std::cout << "Enter as parameters: ./test </> someDate </> somePrice " << std::endl;

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
    // std::cout << "Connecting to GPUdb host: '" << host << "'" << std::endl;
    gpudb::GPUdb gpudb(host, opts);
    std::string relationDate(argv[2]);
    std::string specificDate(argv[3]);
    std::string relationPrice(argv[4]);
    std::string specificPrice(argv[5]);
    run(gpudb, relationDate, specificDate, relationPrice, specificPrice);
    return 0;
}
