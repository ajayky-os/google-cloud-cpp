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
#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace gcs = ::google::cloud::storage;

struct LatencyRecord {
  std::chrono::system_clock::time_point timestamp;
  std::int64_t size_bytes;
  std::int64_t offset;
  std::chrono::microseconds open_duration;
  std::chrono::microseconds read_duration;
  std::chrono::microseconds total_duration;
  std::chrono::microseconds max_chunk_duration;
  std::size_t chunks_count;
  bool success;
  std::string status_code;
};

void PrintPercentiles(std::string const& label,
                      std::vector<std::chrono::microseconds>& latencies) {
  if (latencies.empty()) {
    std::cout << label << ": No data.\n";
    return;
  }
  std::sort(latencies.begin(), latencies.end());

  auto get_percentile = [&](double p) -> double {
    auto idx = static_cast<std::size_t>(p * latencies.size());
    if (idx >= latencies.size()) idx = latencies.size() - 1;
    return latencies[idx].count() / 1000.0;
  };

  std::cout << std::fixed << std::setprecision(2);
  std::cout << "--- " << label << " (ms) ---\n";
  std::cout << "  Count: " << latencies.size() << "\n";
  std::cout << "  Min:   " << (latencies.front().count() / 1000.0) << " ms\n";
  std::cout << "  p50:   " << get_percentile(0.50) << " ms\n";
  std::cout << "  p90:   " << get_percentile(0.90) << " ms\n";
  std::cout << "  p95:   " << get_percentile(0.95) << " ms\n";
  std::cout << "  p99:   " << get_percentile(0.99) << " ms\n";
  std::cout << "  p99.9: " << get_percentile(0.999) << " ms\n";
  std::cout << "  Max:   " << (latencies.back().count() / 1000.0) << " ms\n";
}

void PrintGroupStats(std::string const& title,
                     std::vector<LatencyRecord> const& records) {
  std::vector<std::chrono::microseconds> total_latencies;
  std::vector<std::chrono::microseconds> open_latencies;
  std::vector<std::chrono::microseconds> read_latencies;
  std::vector<std::chrono::microseconds> max_chunk_latencies;

  std::size_t success_count = 0;
  std::size_t failure_count = 0;

  total_latencies.reserve(records.size());
  open_latencies.reserve(records.size());
  read_latencies.reserve(records.size());
  max_chunk_latencies.reserve(records.size());

  for (auto const& r : records) {
    if (r.success) {
      ++success_count;
      total_latencies.push_back(r.total_duration);
      open_latencies.push_back(r.open_duration);
      read_latencies.push_back(r.read_duration);
      max_chunk_latencies.push_back(r.max_chunk_duration);
    } else {
      ++failure_count;
    }
  }

  std::cout << "\n================ " << title << " ================\n";
  std::cout << "Total Requests:      " << records.size() << "\n";
  std::cout << "Successful Requests: " << success_count << "\n";
  std::cout << "Failed Requests:     " << failure_count << "\n";

  PrintPercentiles("Total Latency", total_latencies);
  PrintPercentiles("Open Latency (TTFB)", open_latencies);
  PrintPercentiles("Read Latency (Payload)", read_latencies);
  PrintPercentiles("Max Chunk Latency", max_chunk_latencies);
  std::cout << "===================================================\n";
}

void PrintStats(std::vector<LatencyRecord> const& records,
                std::vector<std::int64_t> const& target_sizes) {
  if (records.empty()) {
    std::cout << "No records.\n";
    return;
  }

  PrintGroupStats("Overall Summary", records);

  if (target_sizes.size() > 1) {
    for (std::int64_t size : target_sizes) {
      std::vector<LatencyRecord> group;
      for (auto const& r : records) {
        if (r.size_bytes == size) {
          group.push_back(r);
        }
      }
      std::ostringstream ss;
      ss << "Size Breakdown: " << (size / (1024 * 1024)) << "MB (" << size
         << " bytes)";
      PrintGroupStats(ss.str(), group);
    }
  }
}

void WriteCsv(std::string const& filename,
              std::vector<LatencyRecord> const& records) {
  std::ofstream out(filename);
  if (!out) {
    std::cerr << "Error: Could not open " << filename << " for writing CSV.\n";
    return;
  }
  out << "Timestamp_ms,Size_bytes,Offset,Open_ms,Read_ms,Total_ms,MaxChunk_ms,"
         "Chunks,Success,StatusCode\n";
  for (auto const& r : records) {
    auto ts_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     r.timestamp.time_since_epoch())
                     .count();
    out << ts_ms << "," << r.size_bytes << "," << r.offset << ","
        << (r.open_duration.count() / 1000.0) << ","
        << (r.read_duration.count() / 1000.0) << ","
        << (r.total_duration.count() / 1000.0) << ","
        << (r.max_chunk_duration.count() / 1000.0) << "," << r.chunks_count
        << "," << (r.success ? "1" : "0") << "," << r.status_code << "\n";
  }
  std::cout << "\nRaw latencies written to " << filename << "\n";
}

std::vector<std::int64_t> ParseSizes(std::string const& str) {
  std::vector<std::int64_t> sizes;
  std::stringstream ss(str);
  std::string item;
  while (std::getline(ss, item, ',')) {
    if (item.empty()) continue;
    std::string upper = item;
    for (char& c : upper) c = static_cast<char>(std::toupper(c));
    std::int64_t multiplier = 1;
    if (upper.size() >= 2 && upper.substr(upper.size() - 2) == "MB") {
      multiplier = 1024 * 1024;
      upper = upper.substr(0, upper.size() - 2);
    } else if (upper.size() >= 2 && upper.substr(upper.size() - 2) == "KB") {
      multiplier = 1024;
      upper = upper.substr(0, upper.size() - 2);
    } else if (upper.size() >= 2 && upper.substr(upper.size() - 2) == "GB") {
      multiplier = 1024 * 1024 * 1024LL;
      upper = upper.substr(0, upper.size() - 2);
    }
    sizes.push_back(std::stoll(upper) * multiplier);
  }
  if (sizes.empty()) {
    sizes.push_back(50 * 1024 * 1024LL);
  }
  return sizes;
}

int main(int argc, char* argv[]) {
  if (argc < 4) {
    std::cerr << "Usage: " << argv[0]
              << " <bucket_name> <object_name> <duration_minutes> "
                 "[concurrency] [read_sizes] [csv_output] "
                 "[enable_hedging] [hedge_delay_ms] [stall_timeout_secs] "
                 "[chunk_size_bytes]\n";
    return 1;
  }

  std::string bucket_name = argv[1];
  std::string object_name = argv[2];
  double duration_minutes = std::stod(argv[3]);
  int concurrency = (argc >= 5) ? std::stoi(argv[4]) : 15;
  std::string sizes_arg = (argc >= 6) ? argv[5] : "2MB,3MB,5MB";
  std::string csv_filename = (argc >= 7) ? argv[6] : "raw_latencies.csv";
  bool enable_hedging = (argc >= 8) ? (std::stoi(argv[7]) != 0) : true;
  int hedge_delay_ms = (argc >= 9) ? std::stoi(argv[8]) : 500;
  int stall_timeout_secs = (argc >= 10) ? std::stoi(argv[9]) : 0;
  std::size_t chunk_size_bytes =
      (argc >= 11) ? std::stoull(argv[10]) : (1024 * 1024ULL);
  int hedge_pool_size = (argc >= 12) ? std::stoi(argv[11]) : 30;

  std::vector<std::int64_t> target_sizes = ParseSizes(sizes_arg);

  auto options =
      google::cloud::Options{}
          .set<google::cloud::storage_experimental::EnableReadHedgingOption>(
              enable_hedging)
          .set<google::cloud::storage_experimental::ReadHedgeDelayOption>(
              std::chrono::milliseconds(hedge_delay_ms))
          .set<google::cloud::storage_experimental::MaxConcurrentHedgesOption>(
              hedge_pool_size)
          .set<google::cloud::storage_experimental::HedgingThreadPoolSizeOption>(
              static_cast<std::size_t>(hedge_pool_size))
          .set<google::cloud::storage_experimental::HttpConnectTimeoutOption>(
              std::chrono::milliseconds(1000))
          .set<google::cloud::storage_experimental::MaximumHedgeBufferOption>(
              8 * 1024 * 1024)
          .set<gcs::BackoffPolicyOption>(
              gcs::ExponentialBackoffPolicy(std::chrono::milliseconds(1),
                                            std::chrono::milliseconds(2), 2.0)
                  .clone())
          .set<gcs::ConnectionPoolSizeOption>(
              (std::max)(concurrency * 2, hedge_pool_size * 2));

  if (stall_timeout_secs > 0) {
    options.set<gcs::DownloadStallTimeoutOption>(
               std::chrono::seconds(stall_timeout_secs))
        .set<gcs::TransferStallTimeoutOption>(
            std::chrono::seconds(stall_timeout_secs))
        .set<gcs::DownloadStallMinimumRateOption>(1);
  }

  auto client = gcs::Client(options);

  std::int64_t object_size = 524288000LL;
  auto meta = client.GetObjectMetadata(bucket_name, object_name);
  if (meta) {
    object_size = static_cast<std::int64_t>(meta->size());
  } else {
    std::cout << "Warning: Could not fetch object metadata (" << meta.status()
              << "), defaulting object size to " << object_size << " bytes.\n";
  }

  std::vector<LatencyRecord> all_records;
  std::mutex records_mutex;
  std::atomic<int> total_iterations{0};

  std::cout << "Starting benchmark for gs://" << bucket_name << "/"
            << object_name << "\n";
  std::cout << "Target Duration: " << duration_minutes << " minutes\n";
  std::cout << "Concurrency:     " << concurrency << " workers\n";
  std::cout << "Object Size:     " << object_size << " bytes\n";
  std::cout << "Read Sizes:      ";
  for (std::size_t i = 0; i < target_sizes.size(); ++i) {
    std::cout << target_sizes[i] << " bytes"
              << (i + 1 < target_sizes.size() ? ", " : "\n");
  }
  std::cout << "Chunk Size:      " << chunk_size_bytes << " bytes\n";
  std::cout << "Hedging Enabled: " << (enable_hedging ? "Yes" : "No") << "\n";
  if (enable_hedging) {
    std::cout << "Hedge Delay:     " << hedge_delay_ms << " ms\n";
    std::cout << "Hedge Pool Size: " << hedge_pool_size << " threads\n";
  }
  if (stall_timeout_secs > 0) {
    std::cout << "Stall Timeout:   " << stall_timeout_secs << " sec\n";
  } else {
    std::cout << "Stall Timeout:   Disabled\n";
  }

  auto test_start = std::chrono::steady_clock::now();
  auto target_duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::duration<double, std::ratio<60>>(duration_minutes));

  auto worker_func = [&](int thread_id) {
    std::vector<LatencyRecord> local_records;
    local_records.reserve(
        static_cast<std::size_t>(duration_minutes * 60 * 100));

    std::vector<char> buffer(chunk_size_bytes);
    std::mt19937_64 rng(std::random_device{}() + thread_id * 10007);
    std::uniform_int_distribution<std::size_t> size_dist(
        0, target_sizes.size() - 1);

    while (true) {
      auto now = std::chrono::steady_clock::now();
      if (now - test_start >= target_duration) {
        break;
      }

      std::int64_t const req_size = target_sizes[size_dist(rng)];
      std::int64_t const max_offset =
          std::max<std::int64_t>(0, object_size - req_size);
      std::uniform_int_distribution<std::int64_t> offset_dist(0, max_offset);
      std::int64_t const req_offset = offset_dist(rng);

      auto start = std::chrono::steady_clock::now();
      auto start_system = std::chrono::system_clock::now();

      auto stream = client.ReadObject(
          bucket_name, object_name,
          gcs::ReadRange(req_offset, req_offset + req_size));

      auto end_open = std::chrono::steady_clock::now();
      auto open_dur = std::chrono::duration_cast<std::chrono::microseconds>(
          end_open - start);

      if (!stream) {
        auto end_err = std::chrono::steady_clock::now();
        auto total_dur = std::chrono::duration_cast<std::chrono::microseconds>(
            end_err - start);
        local_records.push_back(
            {start_system, req_size, req_offset, open_dur,
             std::chrono::microseconds(0), total_dur,
             std::chrono::microseconds(0), 0, false,
             google::cloud::StatusCodeToString(stream.status().code())});
        continue;
      }

      std::size_t chunks_count = 0;
      std::chrono::microseconds max_chunk_dur{0};
      bool read_failed = false;

      while (true) {
        auto chunk_start = std::chrono::steady_clock::now();
        stream.read(buffer.data(), buffer.size());
        auto chunk_end = std::chrono::steady_clock::now();
        std::streamsize bytes_read = stream.gcount();
        if (bytes_read > 0) {
          ++chunks_count;
          auto chunk_dur =
              std::chrono::duration_cast<std::chrono::microseconds>(
                  chunk_end - chunk_start);
          if (chunk_dur > max_chunk_dur) {
            max_chunk_dur = chunk_dur;
          }
        }
        if (!stream) {
          if (stream.bad() && !stream.status().ok()) {
            read_failed = true;
          }
          break;
        }
      }

      auto end_read = std::chrono::steady_clock::now();
      auto read_dur = std::chrono::duration_cast<std::chrono::microseconds>(
          end_read - end_open);
      auto total_dur = std::chrono::duration_cast<std::chrono::microseconds>(
          end_read - start);

      local_records.push_back(
          {start_system, req_size, req_offset, open_dur, read_dur, total_dur,
           max_chunk_dur, chunks_count, !read_failed,
           read_failed
               ? google::cloud::StatusCodeToString(stream.status().code())
               : "OK"});

      int iters = ++total_iterations;
      if (iters % 1000 == 0) {
        auto elapsed_sec =
            std::chrono::duration_cast<std::chrono::seconds>(end_read - test_start)
                .count();
        std::cout << "Completed " << iters << " iterations in " << elapsed_sec
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
    workers.emplace_back(worker_func, i);
  }

  for (auto& w : workers) {
    w.join();
  }

  auto test_end = std::chrono::steady_clock::now();
  auto elapsed_minutes =
      std::chrono::duration_cast<std::chrono::minutes>(test_end - test_start)
          .count();
  std::cout << "\nTest completed after " << elapsed_minutes << " minutes.\n";

  PrintStats(all_records, target_sizes);
  WriteCsv(csv_filename, all_records);

  return 0;
}
