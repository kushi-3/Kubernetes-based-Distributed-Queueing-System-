#include <grpcpp/grpcpp.h>
#include "queue.grpc.pb.h"

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/fmt/fmt.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <prometheus/exposer.h>
#include <prometheus/registry.h>
#include <prometheus/gauge.h>
#include <prometheus/counter.h>

#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>

// Thread safe queue
template <typename T>
class JobQueue {
public:
  // Enqueue a job and notify a waiting thread
  void push(T job) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      queue_.push(std::move(job));
    }
    cv_.notify_one();
  }

  // Dequeue a job and wait if the queue is empty
  T pop() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [&] {
      return !queue_.empty();
    });

    T job = std::move(queue_.front());
    queue_.pop();
    return job;
  }

  // Return the number of jobs in the queue
  std::size_t size() {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size();
  }

private:
  std::queue<T> queue_;
  std::mutex mutex_;
  std::condition_variable cv_;
};


// gRPC job queue service
class JobQueueService final : public jobqueue::JobQueue::Service {
public:
  JobQueueService(JobQueue<jobqueue::Job>& queue,
                  prometheus::Gauge& gauge_queue,
                  prometheus::Counter& counter_errors,
                  const int max_size,
                  const int queue_id)
    : queue_(queue),
      gauge_queue_(gauge_queue),
      counter_errors_(counter_errors),
      max_size_(max_size),
      queue_id_(queue_id) {}

  // RPC handler for submitting a job to the queue
  ::grpc::Status SubmitJob(::grpc::ServerContext* context,
                           const jobqueue::Job* request,
                           jobqueue::SubmitJobResponse* response) override {
    jobqueue::Job job = *request;

    // Get the queue ID this job was submitted from
    std::string via = "generator";
    auto client_metadata = context->client_metadata();
    auto it = client_metadata.find("via-queue-id");
    if (it != client_metadata.end()) {
      via = std::string(it->second.data(), it->second.length());
    }

    spdlog::info("j{} IN q{} ARRIVE-VIA {}", job.id(), queue_id_, via);

    // Enqueue the job unless the queue is full
    if (max_size_ > 0 && queue_.size() >= max_size_) {
      spdlog::error("j{} in q{} QUEUE-FULL: max size = {}", job.id(), queue_id_, max_size_);
      counter_errors_.Increment();
    } else {
      queue_.push(job);
      gauge_queue_.Increment();
    }

    // Respond to client with job ID
    response->set_id(job.id());
    return ::grpc::Status::OK;
  }

private:
  JobQueue<jobqueue::Job>& queue_;

  prometheus::Gauge& gauge_queue_;
  prometheus::Counter& counter_errors_;

  const int max_size_;
  const int queue_id_;
};

// Helper to run the queue server
void RunQueueServer(JobQueue<jobqueue::Job>& queue,
                    prometheus::Gauge& gauge_queue,
                    prometheus::Counter& counter_errors,
                    const std::string& addr,
                    const int max_size,
                    const int queue_id) {
  JobQueueService service(queue, gauge_queue, counter_errors, max_size, queue_id);

  grpc::ServerBuilder builder;
  builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());

  server->Wait();
}


// Dispatcher pops jobs from local queue and distributes to servers
class Dispatcher {
public:
  Dispatcher(JobQueue<jobqueue::Job>& queue,
             prometheus::Gauge& gauge_queue,
             prometheus::Gauge& gauge_service,
             prometheus::Counter& counter_errors,
             std::string server_addr,
             const int queue_id)
    : queue_(queue),
      gauge_queue_(gauge_queue),
      gauge_service_(gauge_service),
      counter_errors_(counter_errors),
      queue_id_(queue_id) {

    auto server_channel = grpc::CreateChannel(server_addr,
                          grpc::InsecureChannelCredentials());
    server_stub_ = jobqueue::JobServer::NewStub(server_channel);

    std::string distributor_addr =
      "distributors-distributor-1.distributors-distributor.distributors.svc.cluster.local:50000";
    auto distributor_channel = grpc::CreateChannel(distributor_addr,
                               grpc::InsecureChannelCredentials());
    distributor_stub_ = jobqueue::JobQueue::NewStub(distributor_channel);
  }

  // Run the dispatcher
  void run() {
    while (true) {
      jobqueue::Job job = queue_.pop();
      gauge_queue_.Decrement();
      sendToServer(job);
    }
  }

private:
  JobQueue<jobqueue::Job>& queue_;

  std::unique_ptr<jobqueue::JobServer::Stub> server_stub_;
  std::unique_ptr<jobqueue::JobQueue::Stub> distributor_stub_;

  prometheus::Gauge& gauge_queue_;
  prometheus::Gauge& gauge_service_;
  prometheus::Counter& counter_errors_;

  const int queue_id_;

  // Send a job to the server
  void sendToServer(const jobqueue::Job& job) {
    grpc::ClientContext ctx;
    jobqueue::ProcessJobResponse resp;

    // Submit the job to the server
    gauge_service_.Increment();
    grpc::Status status = server_stub_->ProcessJob(&ctx, job, &resp);
    gauge_service_.Decrement();

    if (!status.ok()) {
      counter_errors_.Increment();
      spdlog::error("j{} in q{} Server RPC FAILED: {}",
                    job.id(), queue_id_, status.error_message());
      return;
    }

    // If there are more tasks, requeue
    if (resp.job().tasks_size() != 0) {
      const auto& task = resp.job().tasks(0);

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

      // If the operation string is not in the format "op-key-value", requeue the job
      size_t edge_index = 0;
      if (parts.size() != 3) {
        queue_.push(resp.job());
        gauge_queue_.Increment();
      } else {
        route_to_distributor(resp.job());
      }
    } else {
      spdlog::info("j{} IN q{} COMPLETED", job.id(), queue_id_);
    }
  }

  // Route job to a distributor
  void route_to_distributor(const jobqueue::Job& job) {
    spdlog::info("j{} IN q{} DEPART-VIA {}",
                 job.id(), queue_id_, "d1");

    // Add the queue ID to the metadata
    grpc::ClientContext ctx;
    std::string via = fmt::format("q{}", queue_id_);
    ctx.AddMetadata("via-queue-id", via);

    jobqueue::SubmitJobResponse resp;
    grpc::Status status = distributor_stub_->SubmitJob(&ctx, job, &resp);

    if (!status.ok()) {
      counter_errors_.Increment();
      spdlog::error("j{} in q{} Distributor RPC FAILED: {}",
                    job.id(), queue_id_, status.error_message());
      return;
    }
  }
};

// Start multiple dispatchers threads for each server
void StartDispatchers(JobQueue<jobqueue::Job>& queue,
                      prometheus::Gauge& gauge_queue,
                      prometheus::Gauge& gauge_service,
                      prometheus::Counter& counter_errors,
                      const int queue_id,
                      const int num_servers) {

  std::string server_base = fmt::format("q{}-server", queue_id);
  for (int i = 0; i < num_servers; ++i) {
    std::string server_addr = fmt::format("{}-{}.{}:60000", server_base, i, server_base);
    std::thread([&queue, &gauge_queue, &gauge_service, &counter_errors, server_addr, queue_id] {
      Dispatcher dispatcher(queue, gauge_queue, gauge_service, counter_errors, server_addr, queue_id);
      dispatcher.run();
    }).detach();
  }
}


int main(int argc, char* argv[]) {
  if (argc < 4) {
    std::cerr << "Usage: " << argv[0] << " <queue-id> <num-servers> <max-size>\n";
    return EXIT_FAILURE;
  }

  const int queue_id = std::stoi(argv[1]);
  const int num_servers = std::stoi(argv[2]);
  const int max_size = std::stoi(argv[3]);

  std::string addr("0.0.0.0:50000");


  // Prometheus metrics setup
  auto registry = std::make_shared<prometheus::Registry>();

  auto& gauge_queue_family = prometheus::BuildGauge()
                             .Name("queue_jobs_in_queue")
                             .Help("Number of jobs waiting in queue")
                             .Register(*registry);

  auto& gauge_service_family = prometheus::BuildGauge()
                               .Name("queue_jobs_in_service")
                               .Help("Number of jobs currently in service")
                               .Register(*registry);

  auto& counter_error_family = prometheus::BuildCounter()
                               .Name("queue_jobs_errors")
                               .Help("Number of jobs dropped")
                               .Register(*registry);

  std::map<std::string, std::string> labels{
    {"queue_id", std::to_string(queue_id)}
  };

  auto& gauge_queue = gauge_queue_family.Add(labels);
  auto& gauge_service = gauge_service_family.Add(labels);
  auto& counter_errors = counter_error_family.Add(labels);

  prometheus::Exposer exposer{"0.0.0.0:9100"};
  exposer.RegisterCollectable(registry);


  // Create an async logger
  std::size_t max_pending_logs = 8192;
  std::size_t thread_count = 1;
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

  spdlog::debug("q{} running on {}", queue_id, addr);


  // Runtime
  JobQueue<jobqueue::Job> queue;

  StartDispatchers(queue, gauge_queue, gauge_service, counter_errors, queue_id, num_servers);
  RunQueueServer(queue, gauge_queue, counter_errors, addr, max_size, queue_id);

  spdlog::shutdown();
  return EXIT_SUCCESS;
}
