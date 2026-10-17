#include <iostream>
#include <string>
#include <vector>
#include <queue>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <fstream>
#include <sstream>

std::mutex mtx;
std::condition_variable cv_scheduler;
std::thread scheduler_thread;
std::vector<std::thread> worker_threads;

std::string getFormattedTimestamp() {
    auto now = std::chrono::system_clock::now();
    std::time_t time = std::chrono::system_clock::to_time_t(now);

    std::tm tm_buf;
    localtime_s(&tm_buf, &time);

    int hour = tm_buf.tm_hour;
    std::string period = (hour >= 12) ? "PM" : "AM";

    if (hour == 0) hour = 12;
    else if (hour > 12) hour -= 12;

    std::ostringstream oss;
    oss << "("
        << std::setfill('0') << std::setw(2) << tm_buf.tm_mon + 1 << "/"
        << std::setw(2) << tm_buf.tm_mday << "/"
        << tm_buf.tm_year + 1900 << " "
        << std::setw(2) << hour << ":"
        << std::setw(2) << tm_buf.tm_min << ":"
        << std::setw(2) << tm_buf.tm_sec << period << ")";

    return oss.str();
}

struct Process {
    std::string name;
    int total_lines;
    std::atomic<int> current_line{ 0 };
    int assigned_core{ -1 };
    std::string start_time;
    bool is_finished{ false };

    Process(std::string n, int lines)
        : name(std::move(n)), total_lines(lines) {
    }
};

class FCFSScheduler {
public:
    static constexpr int NUM_CORES = 4;

    FCFSScheduler()
        : running(true),
        workers_stop(false),
        core_idle(NUM_CORES, true),
        core_assigned_process(NUM_CORES, nullptr) {

        for (int i = 0; i < NUM_CORES; ++i) {
            worker_threads.emplace_back(
                &FCFSScheduler::cpuWorkerThread, this, i);
        }

        scheduler_thread =
            std::thread(&FCFSScheduler::schedulerLoop, this);
    }

    ~FCFSScheduler() {
        shutdown();
    }

    void addProcess(std::shared_ptr<Process> proc) {
        {
            std::lock_guard<std::mutex> lock(mtx);
            all_processes.push_back(proc);
            ready_queue.push(proc);
        }

        cv_scheduler.notify_one();
    }

    void printStatus() {
        std::lock_guard<std::mutex> lock(mtx);

        std::cout << "--------------------------------------------\n";
        std::cout << "Running processes:\n";

        for (const auto& proc : all_processes) {
            if (!proc->is_finished && proc->assigned_core != -1) {
                std::cout << std::left << std::setw(12) << proc->name
                    << " " << std::setw(26) << proc->start_time
                    << "Core: " << std::setw(4) << proc->assigned_core
                    << std::right << std::setw(6)
                    << proc->current_line.load()
                    << " / " << proc->total_lines << "\n";
            }
        }

        std::cout << "\nFinished processes:\n";

        for (const auto& proc : all_processes) {
            if (proc->is_finished) {
                std::cout << std::left << std::setw(12) << proc->name
                    << " " << std::setw(26) << proc->start_time
                    << std::setw(10) << "Finished"
                    << std::right << std::setw(6)
                    << proc->total_lines
                    << " / " << proc->total_lines << "\n";
            }
        }

        std::cout << "--------------------------------------------\n";
    }

    void shutdown() {
        {
            std::lock_guard<std::mutex> lock(mtx);

            if (!running) return;
            running = false;
        }

        // the scheduler continues processing queued jobs before stopping.
        cv_scheduler.notify_all();

        if (scheduler_thread.joinable()) {
            scheduler_thread.join();
        }

        for (auto& cv : cv_workers) {
            cv.notify_all();
        }

        for (auto& worker : worker_threads) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

private:
    bool running;
    bool workers_stop;

    std::vector<std::shared_ptr<Process>> all_processes;
    std::queue<std::shared_ptr<Process>> ready_queue;

    std::mutex mtx;
    std::condition_variable cv_scheduler;

    std::vector<std::thread> worker_threads;
    std::vector<bool> core_idle;
    std::vector<std::shared_ptr<Process>> core_assigned_process;
    std::condition_variable cv_workers[NUM_CORES];

    bool hasFreeCore() {
        for (int i = 0; i < NUM_CORES; ++i) {
            if (core_idle[i]) return true;
        }
        return false;
    }

    int getNextFreeCore() {
        for (int i = 0; i < NUM_CORES; ++i) {
            if (core_idle[i]) return i;
        }
        return -1;
    }

    bool allProcessesFinished() {
        if (!ready_queue.empty()) return false;

        for (const auto& proc : all_processes) {
            if (!proc->is_finished) return false;
        }

        return true;
    }

    void schedulerLoop() {
        std::unique_lock<std::mutex> lock(mtx);

        while (true) {
            cv_scheduler.wait(lock, [this]() {
                return (!ready_queue.empty() && hasFreeCore()) ||
                    (!running && allProcessesFinished());
                });

            if (!running && allProcessesFinished()) {
                workers_stop = true;
                lock.unlock();

                for (auto& cv : cv_workers) {
                    cv.notify_all();
                }
                return;
            }

            while (!ready_queue.empty() && hasFreeCore()) {
                int core = getNextFreeCore();
                auto proc = ready_queue.front();
                ready_queue.pop();

                core_idle[core] = false;
                proc->assigned_core = core;
                proc->start_time = getFormattedTimestamp();
                core_assigned_process[core] = proc;

                cv_workers[core].notify_one();
            }
        }
    }

    void cpuWorkerThread(int core_id) {
        while (true) {
            std::shared_ptr<Process> proc;

            {
                std::unique_lock<std::mutex> lock(mtx);

                cv_workers[core_id].wait(lock, [this, core_id]() {
                    return workers_stop ||
                        core_assigned_process[core_id] != nullptr;
                    });

                if (workers_stop) return;

                proc = core_assigned_process[core_id];
            }

            // each process writes its own log file.
            std::ofstream outfile(proc->name + ".txt");

            if (outfile.is_open()) {
                outfile << "Process name: " << proc->name << "\n";
                outfile << "Logs:\n\n";

                for (int i = 1; i <= proc->total_lines; ++i) {
                    outfile << getFormattedTimestamp()
                        << " Core:" << core_id
                        << " \"Hello world from " << proc->name
                        << "!\"\n";

                    proc->current_line.store(i);
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(35));
                }

                outfile.close();
            }
            else {
                std::cerr << "Error creating " << proc->name << ".txt\n";
            }

            {
                std::lock_guard<std::mutex> lock(mtx);

                proc->is_finished = true;
                core_assigned_process[core_id] = nullptr;
                core_idle[core_id] = true;
            }

            cv_scheduler.notify_one();
        }
    }
};

int main() {
    FCFSScheduler scheduler;

    // create 10 processes with 100 print commands each.
    for (int i = 1; i <= 10; ++i) {
        std::ostringstream name;
        name << "process" << std::setfill('0') << std::setw(2) << i;

        scheduler.addProcess(
            std::make_shared<Process>(name.str(), 100));
    }

    std::cout << "OS Emulator Initialized with 4 CPU Cores "
        << "and FCFS Scheduler.\n";
    std::cout << "Available commands: 'screen -ls', 'exit'\n";

    std::string command;

    while (true) {
        std::cout << "\nroot@emulator:~# ";

        if (!std::getline(std::cin, command)) {
            scheduler.shutdown();
            break;
        }

        size_t first = command.find_first_not_of(" \t");
        if (first == std::string::npos) continue;

        size_t last = command.find_last_not_of(" \t");
        std::string cmd = command.substr(first, last - first + 1);

        if (cmd == "screen -ls") {
            scheduler.printStatus();
        }
        else if (cmd == "exit") {
            std::cout << "Waiting for processes to finish...\n";
            scheduler.shutdown();
            std::cout << "Emulator closed.\n";
            break;
        }
        else {
            std::cout << "Command not recognized. "
                << "Use 'screen -ls' or 'exit'.\n";
        }
    }

    return 0;
}