#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <unordered_map>
#include <filesystem>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
// Function to parse command line arguments
bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date, int& num_threads, std::string& table_path, std::string& result_path) {
    // TODO: Implement command line argument parsing
    // Example: --r_name ASIA --start_date 1994-01-01 --end_date 1995-01-01 --threads 4 --table_path /path/to/tables --result_path /path/to/results
       if (argc != 7) {
        return false;
    }

    r_name       = argv[1];
    start_date   = argv[2];
    end_date     = argv[3];
    num_threads  = std::atoi(argv[4]);
    table_path  = argv[5];
    result_path = argv[6];

    if (num_threads <= 0) {
        return false;
    }

    return true; 
}

bool readTable(const std::string& file_path,
               const std::vector<std::string>& columns,
               std::vector<std::map<std::string, std::string>>& data) {

    std::ifstream file(file_path);
    if (!file.is_open()) {
        std::cerr << "Failed to open file: " << file_path << std::endl;
        return false;
    }

    std::string line;
    while (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string token;
        std::map<std::string, std::string> row;

        for (const auto& col : columns) {
            if (!std::getline(ss, token, '|')) {
                token = "";
            }
            row[col] = token;
        }

        data.push_back(row);
    }

    return true;
}
// Function to read TPCH data from the specified paths
bool readTPCHData(const std::string& table_path, std::vector<std::map<std::string, std::string>>& customer_data, std::vector<std::map<std::string, std::string>>& orders_data, std::vector<std::map<std::string, std::string>>& lineitem_data, std::vector<std::map<std::string, std::string>>& supplier_data, std::vector<std::map<std::string, std::string>>& nation_data, std::vector<std::map<std::string, std::string>>& region_data) {
    // TODO: Implement reading TPCH data from files
    bool ok = true;

    ok &= readTable(
        table_path + "/customer.tbl",
        {"c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"},
        customer_data
    );

    ok &= readTable(
        table_path + "/orders.tbl",
        {"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"},
        orders_data
    );

    ok &= readTable(
        table_path + "/lineitem.tbl",
        {"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice",
         "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate",
         "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"},
        lineitem_data
    );

    ok &= readTable(
        table_path + "/supplier.tbl",
        {"s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"},
        supplier_data
    );

    ok &= readTable(
        table_path + "/nation.tbl",
        {"n_nationkey", "n_name", "n_regionkey", "n_comment"},
        nation_data
    );

    ok &= readTable(
        table_path + "/region.tbl",
        {"r_regionkey", "r_name", "r_comment"},
        region_data
    );

    return ok;
}


// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date, int num_threads, const std::vector<std::map<std::string, std::string>>& customer_data, const std::vector<std::map<std::string, std::string>>& orders_data, const std::vector<std::map<std::string, std::string>>& lineitem_data, const std::vector<std::map<std::string, std::string>>& supplier_data, const std::vector<std::map<std::string, std::string>>& nation_data, const std::vector<std::map<std::string, std::string>>& region_data, std::map<std::string, double>& results) {
    // TODO: Implement TPCH Query 5 using multithreading
     auto start = std::chrono::high_resolution_clock::now();
	results.clear();


    // 1. Region lookup
    std::string region_key;
    for (const auto& r : region_data) {
        if (r.at("r_name") == r_name) {
            region_key = r.at("r_regionkey");
            break;
        }
    }
    if (region_key.empty()) return false;

   
    // 2. Nation maps
    std::unordered_map<std::string, std::string> nationkey_to_name;
    for (const auto& n : nation_data) {
        if (n.at("n_regionkey") == region_key) {
            nationkey_to_name[n.at("n_nationkey")] = n.at("n_name");
        }
    }

    // 3. Supplier → Nation
    std::unordered_map<std::string, std::string> supplier_to_nation;
    for (const auto& s : supplier_data) {
        if (nationkey_to_name.count(s.at("s_nationkey"))) {
            supplier_to_nation[s.at("s_suppkey")] = s.at("s_nationkey");
        }
    }

    // 4. Customer → Nation
   
    std::unordered_map<std::string, std::string> customer_to_nation;
    for (const auto& c : customer_data) {
        customer_to_nation[c.at("c_custkey")] = c.at("c_nationkey");
    }

   
    // 5. Orders in date range
    std::unordered_map<std::string, std::string> order_to_customer;
    for (const auto& o : orders_data) {
        const auto& d = o.at("o_orderdate");
        if (d >= start_date && d < end_date) {
            order_to_customer[o.at("o_orderkey")] = o.at("o_custkey");
        }
    }

    
    // 6. Multithreaded Lineitem Processing
    std::mutex result_mutex;
    std::vector<std::thread> threads;

    size_t total = lineitem_data.size();
    size_t chunk = total / num_threads;

    auto worker = [&](size_t begin, size_t end) {
        std::unordered_map<std::string, double> local_result;

        for (size_t i = begin; i < end; ++i) {
            const auto& l = lineitem_data[i];

            const std::string& orderkey = l.at("l_orderkey");
            const std::string& suppkey  = l.at("l_suppkey");

            if (!order_to_customer.count(orderkey)) continue;
            if (!supplier_to_nation.count(suppkey)) continue;

            const std::string& custkey = order_to_customer.at(orderkey);
            const std::string& cust_nation = customer_to_nation.at(custkey);
            const std::string& supp_nation = supplier_to_nation.at(suppkey);

            if (cust_nation != supp_nation) continue;

            double price = std::stod(l.at("l_extendedprice"));
            double disc  = std::stod(l.at("l_discount"));

            local_result[cust_nation] += price * (1.0 - disc);
        }

        // Merge local → global
        std::lock_guard<std::mutex> lock(result_mutex);
        for (const auto& kv : local_result) {
            results[nationkey_to_name[kv.first]] += kv.second;
        }
    };

    // Spawn threads
    for (int t = 0; t < num_threads; ++t) {
        size_t start = t * chunk;
        size_t end   = (t == num_threads - 1) ? total : start + chunk;
        threads.emplace_back(worker, start, end);
    }

    // Join threads
    for (auto& th : threads) {
        th.join();
    }
auto end = std::chrono::high_resolution_clock::now();
std::cout << "Time: "
          << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
          << " ms\n";
    return true;
}

// Function to output results to the specified path
bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) {
    // TODO: Implement outputting results to a file
     struct stat st;
    if (stat(result_path.c_str(), &st) != 0) {
        if (mkdir(result_path.c_str(), 0755) != 0) {
            perror("mkdir failed");
            return false;
        }
    }

    std::string output_file = result_path + "/query5_result.txt";
    std::ofstream out(output_file);

    if (!out.is_open()) {
        std::cerr << "Failed to open file: " << output_file << std::endl;
        return false;
    }

    out << "Nation|Revenue\n";
    out << "---------------------------\n";

    for (const auto& kv : results) {
        out << kv.first << "|" << kv.second << "\n";
    }

    out.close();

    std::cout << "Results written to: " << output_file << std::endl;
    return true;
} 
