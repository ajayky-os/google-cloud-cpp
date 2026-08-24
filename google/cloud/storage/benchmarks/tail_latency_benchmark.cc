// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "google/cloud/storage/client.h"
#include "google/cloud/storage/options.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace gcs = ::google::cloud::storage;

struct LatencyRecord {
  std::chrono::system_clock::time_point timestamp;
  std::chrono::microseconds open_duration;
  std::chrono::microseconds read_duration;
  std::chrono::microseconds total_duration;
  bool success;
  std::string status_code;
};

void PrintPercentiles(std::string const& label,
                      std::vector<std::chrono::microseconds> latencies) {
  if (latencies.empty()) {
    std::cout << "No " << label << " latencies recorded.\n";
    return;
  }
  std::sort(latencies.begin(), latencies.end());

  auto get_p = [&](double p) {
    auto idx = static_cast<std::size_t>(p * latencies.size());
    if (idx >= latencies.size()) idx = latencies.size() - 1;
    return latencies[idx].count() / 1000.0;
  };

  std::cout << "\n--- " << label << " Statistics (ms) ---\n";
  std::cout << "Count: " << latencies.size() << "\n";
  std::cout << "Min:   " << latencies.front().count() / 1000.0 << " ms\n";
  std::cout << "p50:   " << get_p(0.50) << " ms\n";
  std::cout << "p90:   " << get_p(0.90) << " ms\n";
  std::cout << "p95:   " << get_p(0.95) << " ms\n";
  std::cout << "p99:   " << get_p(0.99) << " ms\n";
  std::cout << "p99.9: " << get_p(0.999) << " ms\n";
  std::cout << "Max:   " << latencies.back().count() / 1000.0 << " ms\n";
}

void PrintStats(std::vector<LatencyRecord> const& records) {
  if (records.empty()) {
    std::cout << "No records found.\n";
    return;
  }

  std::vector<std::chrono::microseconds> total_latencies;
  std::vector<std::chrono::microseconds> open_latencies;
  std::vector<std::chrono::microseconds> read_latencies;
  std::size_t success_count = 0;
  std::size_t failure_count = 0;

  for (auto const& r : records) {
    if (r.success) {
      ++success_count;
      total_latencies.push_back(r.total_duration);
      open_latencies.push_back(r.open_duration);
      read_latencies.push_back(r.read_duration);
    } else {
      ++failure_count;
    }
  }

  std::cout << "\n================ Benchmark Summary ================\n";
  std::cout << "Total Requests:      " << records.size() << "\n";
  std::cout << "Successful Requests: " << success_count << "\n";
  std::cout << "Failed Requests:     " << failure_count << "\n";

  PrintPercentiles("Total Latency", total_latencies);
  PrintPercentiles("Open Latency (TTFB)", open_latencies);
  PrintPercentiles("Read Latency (Payload)", read_latencies);
  std::cout << "===================================================\n";
}

void WriteCsv(std::string const& filename,
              std::vector<LatencyRecord> const& records) {
  std::ofstream out(filename);
  if (!out) {
    std::cerr << "Error: Could not open " << filename << " for writing CSV.\n";
    return;
  }
  out << "Timestamp_ms,Open_ms,Read_ms,Total_ms,Success,StatusCode\n";
  for (auto const& r : records) {
    auto ts_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     r.timestamp.time_since_epoch())
                     .count();
    out << ts_ms << "," << (r.open_duration.count() / 1000.0) << ","
        << (r.read_duration.count() / 1000.0) << ","
        << (r.total_duration.count() / 1000.0) << ","
        << (r.success ? "1" : "0") << "," << r.status_code << "\n";
  }
  std::cout << "\nRaw latencies written to " << filename << "\n";
}

int main(int argc, char* argv[]) {
  if (argc < 4) {
    std::cerr << "Usage: " << argv[0]
              << " <bucket_name> <object_name> <duration_minutes> "
                 "[concurrency] [read_size_bytes] [csv_output] "
                 "[enable_hedging] [hedge_delay_ms]\n";
    return 1;
  }

  std::string bucket_name = argv[1];
  std::string object_name = argv[2];
  int duration_minutes = std::stoi(argv[3]);
  int concurrency = (argc >= 5) ? std::stoi(argv[4]) : 30;
  long long read_size_bytes =
      (argc >= 6) ? std::stoll(argv[5]) : (1024 * 1024);
  std::string csv_filename = (argc >= 7) ? argv[6] : "raw_latencies.csv";
  bool enable_hedging = (argc >= 8) ? (std::stoi(argv[7]) != 0) : true;
  int hedge_delay_ms = (argc >= 9) ? std::stoi(argv[8]) : 500;

  auto options =
      google::cloud::Options{}
          .set<google::cloud::storage_experimental::EnableReadHedgingOption>(
              enable_hedging)
          .set<google::cloud::storage_experimental::ReadHedgeDelayOption>(
              std::chrono::milliseconds(hedge_delay_ms))
          .set<google::cloud::storage_experimental::MaxConcurrentHedgesOption>(
              concurrency / 2 > 0 ? concurrency / 2 : 1)
          .set<google::cloud::storage_experimental::HttpConnectTimeoutOption>(
              std::chrono::milliseconds(1000))
          .set<gcs::DownloadStallTimeoutOption>(std::chrono::seconds(1))
          .set<gcs::TransferStallTimeoutOption>(std::chrono::seconds(1))
          .set<gcs::DownloadStallMinimumRateOption>(1)
          .set<gcs::BackoffPolicyOption>(
              gcs::ExponentialBackoffPolicy(std::chrono::milliseconds(1),
                                            std::chrono::milliseconds(2), 2.0)
                  .clone())
          .set<gcs::ConnectionPoolSizeOption>(concurrency);
  auto client = gcs::Client(options);

  std::vector<LatencyRecord> all_records;
  std::mutex records_mutex;
  std::atomic<int> total_iterations{0};

  std::cout << "Starting benchmark for gs://" << bucket_name << "/"
            << object_name << "\n";
  std::cout << "Target Duration: " << duration_minutes << " minutes\n";
  std::cout << "Concurrency:     " << concurrency << " workers\n";
  std::cout << "Read Size:       "
            << (read_size_bytes > 0 ? std::to_string(read_size_bytes) + " bytes"
                                    : "Full Object")
            << "\n";
  std::cout << "Hedging Enabled: " << (enable_hedging ? "Yes" : "No") << "\n";
  if (enable_hedging) {
    std::cout << "Hedge Delay:     " << hedge_delay_ms << " ms\n";
  }
  std::cout << "Download Stall Timeout: 1 sec\n";

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
        stream = client.ReadObject(bucket_name, object_name,
                                   gcs::ReadRange(0, read_size_bytes));
      } else {
        stream = client.ReadObject(bucket_name, object_name);
      }

      auto end_open = std::chrono::steady_clock::now();

      if (!stream) {
        auto end_err = std::chrono::steady_clock::now();
        auto open_dur = std::chrono::duration_cast<std::chrono::microseconds>(
            end_open - start);
        auto total_dur = std::chrono::duration_cast<std::chrono::microseconds>(
            end_err - start);
        local_records.push_back(
            {start_system, open_dur, std::chrono::microseconds(0), total_dur,
             false, google::cloud::StatusCodeToString(stream.status().code())});
        continue;
      }

      while (stream.read(buffer.data(), buffer.size())) {
      }

      auto end_read = std::chrono::steady_clock::now();

      if (stream.bad() && !stream.status().ok()) {
        auto open_dur = std::chrono::duration_cast<std::chrono::microseconds>(
            end_open - start);
        auto read_dur = std::chrono::duration_cast<std::chrono::microseconds>(
            end_read - end_open);
        auto total_dur = std::chrono::duration_cast<std::chrono::microseconds>(
            end_read - start);
        local_records.push_back(
            {start_system, open_dur, read_dur, total_dur, false,
             google::cloud::StatusCodeToString(stream.status().code())});
        continue;
      }

      auto open_dur = std::chrono::duration_cast<std::chrono::microseconds>(
          end_open - start);
      auto read_dur = std::chrono::duration_cast<std::chrono::microseconds>(
          end_read - end_open);
      auto total_dur = std::chrono::duration_cast<std::chrono::microseconds>(
          end_read - start);

      local_records.push_back(
          {start_system, open_dur, read_dur, total_dur, true, "OK"});

      int current_total = ++total_iterations;
      if (current_total % 1000 == 0) {
        auto elapsed_secs = std::chrono::duration_cast<std::chrono::seconds>(
                                now - test_start)
                                .count();
        std::cout << "Completed " << current_total
                  << " iterations across all workers in " << elapsed_secs
                  << " seconds.\n";
      }
    }

    std::lock_guard<std::mutex> lock(records_mutex);
    all_records.insert(all_records.end(), local_records.begin(),
                       local_records.end());
  };

  std::vector<std::thread> workers;
  workers.reserve(concurrency);
  for (int i = 0; i < concurrency; ++i) {
    workers.emplace_back(worker_func);
  }

  for (auto& w : workers) {
    w.join();
  }

  std::cout << "\nTest completed after " << duration_minutes << " minutes.\n";
  PrintStats(all_records);
  WriteCsv(csv_filename, all_records);

  return 0;
}
