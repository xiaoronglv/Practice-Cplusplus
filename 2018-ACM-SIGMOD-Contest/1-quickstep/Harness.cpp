#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

constexpr std::size_t MAX_FAILED_QUERIES = 100;

static void Usage() {
  std::cerr << "Usage: harness <init-file> <workload-file>"
            << " <result-file> <test-executable>" << std::endl;
}

// Set a file descriptor to be non-blocking
static int SetNonblocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) {
    return flags;
  }
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// Write a given number of bytes to the specified file descriptor
static ssize_t WriteBytes(int fd, const void *buffer, size_t num_bytes) {
  const char *p = (const char *)buffer;
  const char *end = p + num_bytes;
  while (p != end) {
    ssize_t res = write(fd, p, end - p);
    if (res < 0) {
      if (errno == EINTR) continue;
      return res;
    }
    p += res;
  }
  return num_bytes;
}

static void FatalError(const std::string &message) {
  std::perror(message.c_str());
  std::exit(EXIT_FAILURE);
}

int main(int argc, char *argv[]) {
  // Check for the correct number of arguments
  if (argc != 5) {
    Usage();
    std::exit(EXIT_FAILURE);
  }

  std::vector<std::string> input_batches;
  std::vector<std::vector<std::string> > result_batches;

  // Load the workload and result files and parse them into batches
  {
    std::ifstream work_file(argv[2]);
    if (!work_file) {
      std::cerr << "Cannot open workload file" << std::endl;
      exit(EXIT_FAILURE);
    }

    std::ifstream result_file(argv[3]);
    if (!result_file) {
      std::cerr << "Cannot open result file" << std::endl;
      exit(EXIT_FAILURE);
    }

    std::string input_chunk;
    input_chunk.reserve(100000);

    std::vector<std::string> result_chunk;
    result_chunk.reserve(150);

    std::string line;
    while (std::getline(work_file, line)) {
      input_chunk += line;
      input_chunk += '\n';

      if (line.length() > 0 && line[0] != 'F') {
        // Add result
        std::string result;
        std::getline(result_file, result);
        result_chunk.emplace_back(std::move(result));
      } else {
        // End of batch
        // Copy input and results
        input_batches.push_back(input_chunk);
        result_batches.push_back(result_chunk);
        input_chunk = "";
        result_chunk.clear();
      }
    }
  }

  // Create pipes for child communication
  int stdin_pipe[2];
  int stdout_pipe[2];
  if (pipe(stdin_pipe) == -1 || pipe(stdout_pipe) == -1) {
    FatalError("pipe");
  }

  // Start the test executable
  pid_t pid = fork();
  if (pid == -1) {
    FatalError("fork");
  } else if (pid == 0) {
    dup2(stdin_pipe[0], STDIN_FILENO);
    close(stdin_pipe[0]);
    close(stdin_pipe[1]);
    dup2(stdout_pipe[1], STDOUT_FILENO);
    close(stdout_pipe[0]);
    close(stdout_pipe[1]);
    execlp(argv[4], argv[4], nullptr);
    FatalError("execlp");
  }
  close(stdin_pipe[0]);
  close(stdout_pipe[1]);

  std::cerr << "[Server] Sending relation file names ..." << std::endl;

  // Open the file and feed the initial relations
  int init_file = open(argv[1], O_RDONLY);
  if (init_file == -1) {
    std::cerr << "[Server] Cannot open init file" << std::endl;
    std::exit(EXIT_FAILURE);
  }

  while (true) {
    char buffer[4096];
    ssize_t bytes = read(init_file, buffer, sizeof(buffer));
    if (bytes < 0) {
      if (errno == EINTR) {
        continue;
      }
      FatalError("read");
    }
    if (bytes == 0) break;
    ssize_t written = WriteBytes(stdin_pipe[1], buffer, bytes);
    if (written < 0) {
      FatalError("write");
    }
  }

  close(init_file);

  // Signal the end of the initial phase
  ssize_t status_bytes = WriteBytes(stdin_pipe[1], "Done\n", 5);
  if (status_bytes < 0) {
    FatalError("write");
  }

  std::cerr << "[Server] Sleeping for 1 second ..." << std::endl;

  using std::chrono_literals::operator""s;
  std::this_thread::sleep_for(1s);

  // Use select with non-blocking files to read and write from the child process, avoiding deadlocks
  if (SetNonblocking(stdout_pipe[0]) == -1) {
    FatalError("fcntl");
  }

  if (SetNonblocking(stdin_pipe[1]) == -1) {
    FatalError("fcntl");
  }

  // Start the stopwatch
  struct timeval start;
  gettimeofday(&start, NULL);

  std::size_t query_no = 0;
  std::size_t failure_cnt = 0;

  // Loop over all batches
  for (std::size_t batch = 0;
       batch < input_batches.size() && failure_cnt < MAX_FAILED_QUERIES;
       ++batch) {
    std::cerr << "[Server] Sending query batch " << batch << " ..." << std::endl;

    std::string output;  // raw output is collected here
    output.reserve(1000000);

    std::size_t input_ofs = 0;    // byte position in the input batch
    std::size_t output_read = 0;  // number of lines read from the child output

    while (input_ofs < input_batches[batch].length() ||
           output_read < result_batches[batch].size()) {
      fd_set read_fd, write_fd;
      FD_ZERO(&read_fd);
      FD_ZERO(&write_fd);

      if (input_ofs < input_batches[batch].length()) {
        FD_SET(stdin_pipe[1], &write_fd);
      }
      if (output_read < result_batches[batch].size()) {
        FD_SET(stdout_pipe[0], &read_fd);
      }

      int retval = select(std::max(stdin_pipe[1], stdout_pipe[0]) + 1,
                          &read_fd, &write_fd, NULL, NULL);
      if (retval == -1) {
        FatalError("select");
      }

      // Read output from the test program
      if (FD_ISSET(stdout_pipe[0], &read_fd)) {
        char buffer[4096];
        int bytes = read(stdout_pipe[0], buffer, sizeof(buffer));
        if (bytes < 0) {
          if (errno == EINTR) {
            continue;
          }
          FatalError("read");
        }
        // Count how many lines were returned
        for (int j = 0; j < bytes; ++j) {
          if (buffer[j] == '\n') {
            ++output_read;
          }
        }
        output.append(buffer, bytes);
      }

      // Feed another chunk of data from this batch to the test program
      if (FD_ISSET(stdin_pipe[1], &write_fd)) {
        int bytes = write(stdin_pipe[1],
                          input_batches[batch].data() + input_ofs,
                          input_batches[batch].length() - input_ofs);
        if (bytes < 0) {
          if (errno == EINTR) {
            continue;
          }
          FatalError("write");
        }
        input_ofs += bytes;
      }
    }

    // Parse and compare the batch result
    std::stringstream result(output);

    for (std::size_t i = 0;
         i < result_batches[batch].size() && failure_cnt < MAX_FAILED_QUERIES;
         ++i) {
      std::string val;

      std::getline(result, val);
      if (!result) {
        std::cerr << "[Server] Incomplete batch output for batch "
                  << batch << std::endl;
        std::exit(EXIT_FAILURE);
      }

      bool matched = (val == result_batches[batch][i]);
      if (!matched) {
        std::cerr << "[Server] Result mismatch for query " << query_no
                  << ", expected: " << result_batches[batch][i]
                  << ", actual: " << val << std::endl;
        ++failure_cnt;
      }
      ++query_no;
    }
  }

  struct timeval end;
  gettimeofday(&end, NULL);

  // Output the elapsed time in milliseconds
  double elapsed_sec = (end.tv_sec - start.tv_sec) +
                       (end.tv_usec - start.tv_usec) / 1000000.0;
  std::cout << "Elaseped time = " << static_cast<std::uint64_t>(elapsed_sec * 1000)
            << " ms" << std::endl;

  if (failure_cnt == 0) {
    return EXIT_SUCCESS;
  }

  return EXIT_FAILURE;
}
