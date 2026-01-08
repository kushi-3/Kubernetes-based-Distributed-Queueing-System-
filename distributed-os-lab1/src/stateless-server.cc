#include <grpcpp/grpcpp.h>
#include "queue.grpc.pb.h"

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <thread>
#include <chrono>
#include <memory>
#include <string>

class JobServer final : public jobqueue::JobServer::Service {
public:
  explicit JobServer(int queue_id, int server_id) : queue_id_(queue_id), server_id_(server_id) {}

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

private:
  int queue_id_;
  int server_id_;
  std::mutex mutex_;
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

  // Setup gRPC server
  std::string addr = "0.0.0.0:60000";
  JobServer service(queue_id, server_id);

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
