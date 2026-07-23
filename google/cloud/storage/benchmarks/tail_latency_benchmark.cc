#include "google/cloud/storage/client.h"
#include <algorithm>
#include <chrono>
#include <iostream>
#include <fstream>
#include <numeric>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <future>
#include <atomic>

namespace gcs = ::google::cloud::storage;

struct LatencyRecord {
  std::chrono::system_clock::time_point timestamp;
  std::chrono::microseconds open_duration;
  std::chrono::microseconds read_duration;
  std::chrono::microseconds total_duration;
};

void PrintStats(std::vector<LatencyRecord>& records) {
  if (records.empty()) {
    std::cout << "No latencies recorded.\n";
    return;
  }
  
  std::vector<std::chrono::microseconds> latencies;
  latencies.reserve(records.size());
  for (const auto& r : records) {
    latencies.push_back(r.total_duration);
  }
  std::sort(latencies.begin(), latencies.end());

  auto get_percentile = [&](double p) {
    auto idx = static_cast<std::size_t>(p * latencies.size());
    if (idx >= latencies.size()) idx = latencies.size() - 1;
    return latencies[idx].count() / 1000.0;
  };

  std::cout << "\n--- Total Latency Statistics (ms) ---\n";
  std::cout << "Count: " << latencies.size() << "\n";
  std::cout << "Min:   " << latencies.front().count() / 1000.0 << " ms\n";
  std::cout << "p50:   " << get_percentile(0.50) << " ms\n";
  std::cout << "p90:   " << get_percentile(0.90) << " ms\n";
  std::cout << "p95:   " << get_percentile(0.95) << " ms\n";
  std::cout << "p99:   " << get_percentile(0.99) << " ms\n";
  std::cout << "Max:   " << latencies.back().count() / 1000.0 << " ms\n";
}

void WriteCsv(const std::string& filename, const std::vector<LatencyRecord>& records) {
  std::ofstream out(filename);
  if (!out) {
    std::cerr << "Error: Could not open " << filename << " for writing CSV.\n";
    return;
  }
  out << "Timestamp_ms,Open_ms,Read_ms,Total_ms\n";
  for (const auto& r : records) {
    auto ts_ms = std::chrono::duration_cast<std::chrono::milliseconds>(r.timestamp.time_since_epoch()).count();
    out << ts_ms << "," 
        << (r.open_duration.count() / 1000.0) << ","
        << (r.read_duration.count() / 1000.0) << ","
        << (r.total_duration.count() / 1000.0) << "\n";
  }
  std::cout << "\nRaw latencies written to " << filename << "\n";
}

int main(int argc, char* argv[]) {
  if (argc < 4) {
    std::cerr << "Usage: " << argv[0] << " <bucket_name> <object_name> <duration_minutes> [concurrency] [read_size_bytes] [csv_output] [enable_hedging] [multiplier]\n";
    return 1;
  }

  std::string bucket_name = argv[1];
  std::string object_name = argv[2];
  int duration_minutes = std::stoi(argv[3]);
  int concurrency = (argc >= 5) ? std::stoi(argv[4]) : 30;
  long long read_size_bytes = (argc >= 6) ? std::stoll(argv[5]) : (1024 * 1024);
  std::string csv_filename = (argc >= 7) ? argv[6] : "raw_latencies.csv";
  bool enable_hedging = (argc >= 8) ? (std::stoi(argv[7]) != 0) : true;
  double multiplier = (argc >= 9) ? std::stod(argv[8]) : 1.5;

  auto options = google::cloud::Options{}
    .set<google::cloud::storage_experimental::EnableReadHedgingOption>(enable_hedging)
    .set<google::cloud::storage_experimental::DynamicHedgeMultiplierOption>(multiplier)
    .set<google::cloud::storage_experimental::ReadHedgeDelayOption>(std::chrono::milliseconds(300))
    .set<gcs::ConnectionPoolSizeOption>(concurrency);
  auto client = gcs::Client(options);

  std::vector<LatencyRecord> all_records;
  std::mutex records_mutex;
  std::atomic<int> total_iterations{0};

  std::cout << "Starting benchmark for gs://" << bucket_name << "/" << object_name << "\n";
  std::cout << "Target Duration: " << duration_minutes << " minutes\n";
  std::cout << "Concurrency: " << concurrency << " workers\n";
  std::cout << "Read Size: " << (read_size_bytes > 0 ? std::to_string(read_size_bytes) : "Full Object") << "\n";
  std::cout << "Hedging Enabled: " << (enable_hedging ? "Yes" : "No") << "\n";
  if (enable_hedging) {
      std::cout << "Hedge Multiplier: " << multiplier << "x\n";
  }

  auto test_start = std::chrono::steady_clock::now();
  auto target_duration = std::chrono::minutes(duration_minutes);

  auto worker_func = [&]() {
    std::vector<LatencyRecord> local_records;
    local_records.reserve(duration_minutes * 60 * 10);
    
    std::vector<char> buffer(1024 * 1024);

    while (true) {
      auto now = std::chrono::steady_clock::now();
      if (now - test_start >= target_duration) {
        break;
      }

      auto start = std::chrono::steady_clock::now();
      auto start_system = std::chrono::system_clock::now();

      gcs::ObjectReadStream stream;
      if (read_size_bytes > 0) {
          stream = client.ReadObject(bucket_name, object_name, gcs::ReadRange(0, read_size_bytes));
      } else {
          stream = client.ReadObject(bucket_name, object_name);
      }
      
      auto end_open = std::chrono::steady_clock::now();

      if (!stream) {
        continue;
      }

      while (stream.read(buffer.data(), buffer.size())) {}
      
      if (stream.bad() && !stream.status().ok()) {
        continue;
      }

      auto end_read = std::chrono::steady_clock::now();
      
      auto open_dur = std::chrono::duration_cast<std::chrono::microseconds>(end_open - start);
      auto read_dur = std::chrono::duration_cast<std::chrono::microseconds>(end_read - end_open);
      auto total_dur = std::chrono::duration_cast<std::chrono::microseconds>(end_read - start);
      
      local_records.push_back({start_system, open_dur, read_dur, total_dur});

      int current_total = ++total_iterations;
      if (current_total % 1000 == 0) {
        auto elapsed_secs = std::chrono::duration_cast<std::chrono::seconds>(now - test_start).count();
        std::cout << "Running... completed " << current_total << " iterations across all workers in " << elapsed_secs << " seconds.\n";
      }
    }
    
    std::lock_guard<std::mutex> lock(records_mutex);
    all_records.insert(all_records.end(), local_records.begin(), local_records.end());
  };

  std::vector<std::thread> workers;
  for (int i = 0; i < concurrency; ++i) {
    workers.emplace_back(worker_func);
  }

  for (auto& w : workers) {
    w.join();
  }

  std::cout << "\nTest completed after " << duration_minutes << " minutes.\n";
  PrintStats(all_records);
  WriteCsv(csv_filename, all_records);

  if (client.connection()->hedge_manager()) {
    std::string filename = std::string("/home/ajayky_google_com/projects/google-cloud-cpp/dynamic_hedge_latencies_") + std::to_string(multiplier) + ".csv";
    client.connection()->hedge_manager()->DumpLatencies(filename);
    auto metrics = client.connection()->hedge_manager()->GetHedgeMetrics();
    std::ofstream out_metrics(std::string("/home/ajayky_google_com/projects/google-cloud-cpp/hedge_metrics_") + std::to_string(multiplier) + ".csv");
    if (out_metrics) {
        out_metrics << "primary_wins,hedge_wins\n";
        out_metrics << metrics.first << "," << metrics.second << "\n";
    }
  }

  return 0;
}
