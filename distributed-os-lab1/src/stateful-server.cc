#include <grpcpp/grpcpp.h>
#include "queue.grpc.pb.h"

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <unordered_map>
#include <shared_mutex>
#include <fstream>
#include <thread>
#include <chrono>
#include <memory>
#include <string>

class KeyValueStore {
public:
  explicit KeyValueStore(const std::string& backing_file)
    : backing_file_(backing_file) {
    load_from_disk();
  }

  void get(const std::string& key, std::string& value) const {
    std::shared_lock lock(mutex_);
    auto it = kv_.find(key);
    if (it == kv_.end()) value = "0";
    value = it->second;
  }

  void put(const std::string& key, const std::string& value) {
    {
      std::unique_lock lock(mutex_);
      kv_[key] = value;
    }
    persist_to_disk();
  }

  void erase(const std::string& key) {
    {
      std::unique_lock lock(mutex_);
      kv_.erase(key);
    }
    persist_to_disk();
  }

private:
  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, std::string> kv_;
  std::string backing_file_;

  void load_from_disk() {
    std::ifstream in(backing_file_);
    if (!in.is_open()) return;

    std::string key, value;
    std::unordered_map<std::string, std::string> tmp;
    while (std::getline(in, key) && std::getline(in, value)) {
      tmp[key] = value;
    }
    {
      std::unique_lock lock(mutex_);
      kv_.swap(tmp);
    }
  }

  void persist_to_disk() const {
    std::unordered_map<std::string, std::string> snapshot;
    {
      std::shared_lock lock(mutex_);
      snapshot = kv_;
    }
    std::ofstream out(backing_file_, std::ios::trunc);
    if (!out.is_open()) return;
    for (const auto& [k, v] : snapshot) {
      out << k << "\n" << v << "\n";
    }
  }
};

class JobServer final : public jobqueue::JobServer::Service {
public:
  explicit JobServer(int queue_id,
                     int server_id,
                     std::vector<std::string> peers,
                     const std::string& kv_file)
    : queue_id_(queue_id),
      server_id_(server_id),
      kv_(kv_file) {
    for (const auto& addr : peers) {
      auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
      peer_stubs_.push_back(jobqueue::JobServer::NewStub(channel));
    }
  }

  grpc::Status ProcessJob(grpc::ServerContext*,
                          const jobqueue::Job* request,
                          jobqueue::ProcessJobResponse* response) override {

    std::lock_guard<std::mutex> lock(mutex_);

    spdlog::info("j{} IN q{} ENTERS-SERVER s{}",
                 request->id(), queue_id_, server_id_);

    // Get the first task
    const auto& task = request->tasks(0);

    spdlog::info("j{} IN q{} TASK-START {} {} {}",
                 request->id(), queue_id_,task.id(), task.operation(), task.processing_time());


    // Split the operation string
    int pos = 0;
    int start = 0;
    std::vector<std::string> parts;
    const std::string& s = task.operation();
    while ((pos = s.find('-', start)) != std::string::npos) {
      parts.emplace_back(s.substr(start, pos - start));
      start = pos + 1;
    }
    parts.emplace_back(s.substr(start));

    if (parts.size() != 3) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid operation");
    }

    std::string op  = parts[0];
    std::string key = parts[1];
    std::string val = parts[2];

    // Execute the operation
    if (op == "GET") {
      std::string stored;
      kv_.get(key, stored);
      *response->mutable_value() = stored;
    } else if (op == "PUT") {
      grpc::Status st = run_2pc_write("PUT", key, val, response);
      if (!st.ok()) return st;
    } else if (op == "DEL") {
      grpc::Status st = run_2pc_write("DEL", key, "", response);
      if (!st.ok()) return st;
    } else {
      std::string stored;
      kv_.get(key, stored);

      int a, b;

      grpc::Status st = safe_stoi(val, a);
      if (!st.ok()) return st;

      st = safe_stoi(stored, b);
      if (!st.ok()) return st;

      int result = 0;
      if (op == "ADD") {
        result = a + b;
      } else if (op == "SUB") {
        result = b - a;
      } else if (op == "MUL") {
        result = a * b;
      } else if (op == "DIV") {
        if (b == 0) {
          return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Divide by zero");
        }
        result = b / a;
      }

      std::string new_val = std::to_string(result);
      st = run_2pc_write("PUT", key, new_val, response);
      if (!st.ok()) return st;
    }

    // Sleep for processing_time seconds
    std::this_thread::sleep_for(std::chrono::seconds(task.processing_time()));

    spdlog::info("j{} IN q{} TASK-STOP {} {} {}",
                 request->id(), queue_id_, task.id(), task.operation(), task.processing_time());

    // Remove the first task and return the job
    *response->mutable_job() = *request;
    response->mutable_job()->mutable_tasks()->DeleteSubrange(0, 1);
    response->set_success(true);

    spdlog::info("j{} IN q{} EXITS-SERVER s{}",
                 request->id(), queue_id_, server_id_);

    return grpc::Status::OK;
  }

  grpc::Status Prepare(grpc::ServerContext*,
                       const jobqueue::ReplicaWrite* req,
                       jobqueue::VoteResponse* resp) override {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    pending_[req->tx_id()] = *req;
    resp->set_vote(true);
    return grpc::Status::OK;
  }

  grpc::Status Commit(grpc::ServerContext*,
                      const jobqueue::ReplicaWrite* req,
                      google::protobuf::Empty*) override {
    if (req->op() == "PUT") {
      kv_.put(req->key(), req->value());
    } else if (req->op() == "DEL") {
      kv_.erase(req->key());
    }
    std::lock_guard<std::mutex> lock(pending_mutex_);
    pending_.erase(req->tx_id());
    return grpc::Status::OK;
  }

  grpc::Status Abort(grpc::ServerContext*,
                     const jobqueue::ReplicaWrite* req,
                     google::protobuf::Empty*) override {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    pending_.erase(req->tx_id());
    return grpc::Status::OK;
  }

private:
  int queue_id_;
  int server_id_;
  std::mutex mutex_;
  std::vector<std::unique_ptr<jobqueue::JobServer::Stub>> peer_stubs_;

  KeyValueStore kv_;
  std::mutex pending_mutex_;
  std::unordered_map<std::string, jobqueue::ReplicaWrite> pending_;

  std::atomic<uint64_t> tx_counter_{0};

  // Generate a unique transaction ID
  std::string make_tx_id() {
    uint64_t c = tx_counter_.fetch_add(1, std::memory_order_relaxed);
    return std::to_string(queue_id_) + "-" +
           std::to_string(server_id_) + "-" +
           std::to_string(c);
  }

  // Run a 2PC write operation
  grpc::Status run_2pc_write(const std::string& op,
                             const std::string& key,
                             const std::string& value,
                             jobqueue::ProcessJobResponse* response) {
    jobqueue::ReplicaWrite write;
    write.set_key(key);
    write.set_value(value);
    write.set_op(op);
    write.set_tx_id(make_tx_id());

    // Phase 1: Prepare
    jobqueue::VoteResponse resp;
    google::protobuf::Empty empty;
    Prepare(nullptr, &write, &resp);
    for (int idx = 0; idx < peer_stubs_.size(); ++idx) {
      grpc::ClientContext ctx;
      jobqueue::VoteResponse vote;
      grpc::Status st = peer_stubs_[idx]->Prepare(&ctx, write, &vote);
      if (!st.ok() || !vote.vote()) {
        // Abort on all
        Abort(nullptr, &write, &empty);
        for (int idx2 = 0; idx2 < peer_stubs_.size(); ++idx2) {
          grpc::ClientContext ctx2;
          google::protobuf::Empty empty;
          peer_stubs_[idx2]->Abort(&ctx2, write, &empty);
        }
        response->set_success(false);
        response->set_error_message("2PC abort");
        return grpc::Status::OK;
      }
    }

    // Phase 2: Commit
    Commit(nullptr, &write, &empty);
    for (int idx = 0; idx < peer_stubs_.size(); ++idx) {
      grpc::ClientContext ctx;
      google::protobuf::Empty empty;
      grpc::Status st = peer_stubs_[idx]->Commit(&ctx, write, &empty);
      if (!st.ok()) {
        spdlog::error("Commit failed on replica {}: {}", idx, st.error_message());
      }
    }

    response->set_success(true);
    return grpc::Status::OK;
  }

  grpc::Status safe_stoi(const std::string& s, int& out) {
    try {
      out = std::stoi(s);
      return grpc::Status::OK;
    } catch (const std::invalid_argument&) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                          "Invalid integer: " + s);
    } catch (const std::out_of_range&) {
      return grpc::Status(grpc::StatusCode::OUT_OF_RANGE,
                          "Integer out of range: " + s);
    }
  }
};

int main(int argc, char* argv[]) {
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <queue-id> <num-servers>\n";
    return EXIT_FAILURE;
  }

  const int queue_id = std::stoi(argv[1]);
  const int num_servers = std::stoi(argv[2]);

  std::string pod_name = std::getenv("POD_NAME");
  auto pos = pod_name.find_last_of('-');
  const int server_id = std::stoi(pod_name.substr(pos + 1));

  // Create an async logger
  std::size_t max_pending_logs = 8192;
  std::size_t thread_count = 4;
  spdlog::init_thread_pool(max_pending_logs, thread_count);

  auto stdout_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
  stdout_sink->set_level(spdlog::level::debug);

  auto logger = std::make_shared<spdlog::async_logger>(
                  "queue_logger",
                  stdout_sink,
                  spdlog::thread_pool(),
                  spdlog::async_overflow_policy::block
                );

  spdlog::set_default_logger(logger);
  spdlog::set_level(spdlog::level::debug);
  spdlog::flush_on(spdlog::level::debug);
  spdlog::set_pattern("[%^%l%$] %v");

  std::string server_base = fmt::format("q{}-server", queue_id);
  std::vector<std::string> peers;
  for (int i = 0; i < num_servers; ++i) {
    if (i == server_id) continue;
    peers.push_back(
      fmt::format("{}-{}.{}:60000", server_base, i, server_base)
    );
  }

  // Setup gRPC server
  std::string addr = "0.0.0.0:60000";
  std::string db_file = fmt::format("/data/queue{}-server{}.db", queue_id, server_id);
  JobServer service(queue_id, server_id, peers, db_file);

  grpc::ServerBuilder builder;
  builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  // Start gRPC server
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  spdlog::debug("s{} running on {}", server_id, addr);
  server->Wait();

  spdlog::shutdown();
  return EXIT_SUCCESS;
}
