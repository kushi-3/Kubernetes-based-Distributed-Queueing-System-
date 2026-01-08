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
                  const int distributor_id)
    : queue_(queue),
      gauge_queue_(gauge_queue),
      counter_errors_(counter_errors),
      distributor_id_(distributor_id) {}

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

    spdlog::info("j{} IN d{} ARRIVE-VIA {}", job.id(), distributor_id_, via);

    // Enqueue the job
    queue_.push(job);
    gauge_queue_.Increment();

    // Respond to client with job ID
    response->set_id(job.id());
    return ::grpc::Status::OK;
  }

private:
  JobQueue<jobqueue::Job>& queue_;

  prometheus::Gauge& gauge_queue_;
  prometheus::Counter& counter_errors_;

  const int distributor_id_;
};

// Helper to run the queue server
void RunQueueServer(JobQueue<jobqueue::Job>& queue,
                    prometheus::Gauge& gauge_queue,
                    prometheus::Counter& counter_errors,
                    const int distributor_id,
                    const std::string& addr) {
  JobQueueService service(queue, gauge_queue, counter_errors, distributor_id);

  grpc::ServerBuilder builder;
  builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());

  server->Wait();
}


// Dispatcher pops jobs from local queue and distributes to edges
class Dispatcher {
public:
  Dispatcher(JobQueue<jobqueue::Job>& queue,
             const std::vector<std::string>& edges_out,
             prometheus::Gauge& gauge_queue,
             prometheus::Counter& counter_errors,
             const int distributor_id)
    : queue_(queue),
      edges_out_(edges_out),
      gauge_queue_(gauge_queue),
      counter_errors_(counter_errors),
      distributor_id_(distributor_id) {

    // Create stubs for each edge
    for (const auto& addr : edges_out_) {
      auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
      edge_stubs_.push_back(jobqueue::JobQueue::NewStub(channel));
    }
  }

  // Run the dispatcher
  void run() {
    while (true) {
      jobqueue::Job job = queue_.pop();
      gauge_queue_.Decrement();
      route_to_next_edge(job);
    }
  }

private:
  JobQueue<jobqueue::Job>& queue_;
  std::vector<std::string> edges_out_;

  std::vector<std::unique_ptr<jobqueue::JobQueue::Stub>> edge_stubs_;

  prometheus::Gauge& gauge_queue_;
  prometheus::Counter& counter_errors_;

  const int distributor_id_;

  // Route job to each queue using a simple hash function
  void route_to_next_edge(const jobqueue::Job& job) {
    const auto& task = job.tasks(0);

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

    // If the operation string is not in the format "op-key-value", hash the job ID
    size_t edge_index = 0;
    if (parts.size() != 3) {
      edge_index = job.id() % edge_stubs_.size();
    } else {
      edge_index = std::stoi(parts[1]) % edge_stubs_.size();
    }

    auto& edge_stub = edge_stubs_[edge_index];

    // Get the edge label from the address
    const auto& edge_addr = edges_out_[edge_index];
    std::string edge_label = edge_addr.substr(0, edge_addr.find("-"));

    spdlog::info("j{} IN {} DEPART-VIA d{}",
                 job.id(), edge_label, distributor_id_);

    // Add the queue ID to the metadata
    grpc::ClientContext ctx;
    std::string via = fmt::format("d{}", distributor_id_);
    ctx.AddMetadata("via-queue-id", via);

    // Submit the job to the edge
    jobqueue::SubmitJobResponse resp;
    grpc::Status status = edge_stub->SubmitJob(&ctx, job, &resp);

    if (!status.ok()) {
      counter_errors_.Increment();
      spdlog::error("j{} in distributor Edge RPC FAILED: {}",
                    job.id(), status.error_message());
    }
  }
};

// Start dispatcher thread
void StartDispatcher(JobQueue<jobqueue::Job>& queue,
                     const std::vector<std::string>& edges_out,
                     prometheus::Gauge& gauge_queue,
                     prometheus::Counter& counter_errors,
                     const int distributor_id) {
  std::thread([&queue, edges_out, &gauge_queue, &counter_errors, distributor_id] {
    Dispatcher dispatcher(queue, edges_out, gauge_queue, counter_errors, distributor_id);
    dispatcher.run();
  }).detach();
}


int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <num-queues>\n";
    return EXIT_FAILURE;
  }

  const int num_queues = std::stoi(argv[1]);

  std::string pod_name = std::getenv("POD_NAME");
  auto pos = pod_name.find_last_of('-');
  const int distributor_id = std::stoi(pod_name.substr(pos + 1));

  std::string addr("0.0.0.0:50000");


  // Prometheus metrics setup
  auto registry = std::make_shared<prometheus::Registry>();

  auto& gauge_queue_family = prometheus::BuildGauge()
                             .Name("queue_jobs_in_queue")
                             .Help("Number of jobs waiting in distributor")
                             .Register(*registry);

  auto& counter_error_family = prometheus::BuildCounter()
                               .Name("queue_jobs_errors")
                               .Help("Number of jobs dropped")
                               .Register(*registry);

  std::map<std::string, std::string> labels{
    {"queue_id", "distributor"}
  };

  auto& gauge_queue = gauge_queue_family.Add(labels);
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
                  "distributor_logger",
                  stdout_sink,
                  spdlog::thread_pool(),
                  spdlog::async_overflow_policy::block
                );

  spdlog::set_default_logger(logger);
  spdlog::set_level(spdlog::level::debug);
  spdlog::flush_on(spdlog::level::debug);
  spdlog::set_pattern("[%^%l%$] %v");

  spdlog::debug("d{} running on {}", distributor_id, addr);


  // Build the list of edge addresses
  std::vector<std::string> edges_out;
  for (int edge_id = 0; edge_id < num_queues; ++edge_id) {
    std::string hostname = fmt::format("q{}-queue.q{}", edge_id, edge_id);
    edges_out.push_back(hostname + ":50000");
  }

  // Runtime
  JobQueue<jobqueue::Job> queue;

  StartDispatcher(queue, edges_out, gauge_queue, counter_errors, distributor_id);
  RunQueueServer(queue, gauge_queue, counter_errors, distributor_id, addr);

  spdlog::shutdown();
  return EXIT_SUCCESS;
}
