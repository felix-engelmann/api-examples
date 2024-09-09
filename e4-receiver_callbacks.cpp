// Instead of the slsReceiver
#include <sls/Receiver.h>

#include <array>
#include <unordered_map>

#include <csignal> //SIGINT
#include <iostream>
#include <sstream>
#include <semaphore.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <mutex>
#include <thread>
#include <tuple>
#include <set>

#include <zmq.h>

sem_t semaphore;

void sigInterruptHandler(int p) { sem_post(&semaphore); }

const int NUM_STREAMS = 2;

struct Status{
  void *responder;
  int msg_number; 

  bool terminate = false;

  sem_t available;

  std::mutex mtx;
  //std::array<std::unordered_map<long unsigned int, std::tuple< zmq_msg_t *, zmq_msg_t*> >, NUM_STREAMS> cache;
  std::array<std::unordered_map<long unsigned int, zmq_msg_t* >, NUM_STREAMS> cache;
};

// Function to find common keys in an array of unordered_maps and return them sorted
std::vector<int> getCommonKeysSorted(std::array<std::unordered_map<long unsigned int, zmq_msg_t*>, NUM_STREAMS> maps) {
    std::vector<int> commonKeys;

    // Use the first map to initialize the common set of keys
    std::set<int> keysInFirstMap;
    for (const auto& pair : maps[0]) {
        keysInFirstMap.insert(pair.first);
    }

    // Iterate through the remaining maps to find common keys
    for (int i = 1; i < NUM_STREAMS; ++i) {
        std::set<int> currentMapKeys;
        for (const auto& pair : maps[i]) {
            if (keysInFirstMap.find(pair.first) != keysInFirstMap.end()) {
                currentMapKeys.insert(pair.first);  // Key is common
            }
        }
        keysInFirstMap = currentMapKeys;  // Update the common keys set
    }

    // Move common keys into a vector
    commonKeys.assign(keysInFirstMap.begin(), keysInFirstMap.end());

    return commonKeys;  // Already sorted due to std::set
}


void my_free (void *data, void *hint)
{
    //printf("myfree %x\n", data);
    //free (data);
}

void Correlate(Status *stat) {
  int done  = -1;
  while (!stat->terminate) {
    sem_wait(&(stat->available));
    std::cout << "Correlate cache" << std::endl;
    {
      std::lock_guard<std::mutex> lock(stat->mtx);

      for (const auto& sc : stat->cache) {
        std::cout << "in cache for stream" << std::endl;
        for (const auto& pair : sc) {
            std::cout << "frame for trigger" << pair.first << std::endl;
            printf("payload pointer %x\n", pair.second);
        }
      }
      auto keys = getCommonKeysSorted(stat->cache);
      for (const auto k: keys) {
        std::cout << "common key " << k << std::endl;
        if (k <= done) {
          continue;
        }
        for (int i=0; i<NUM_STREAMS; i++) {
          std::cout << "send out stream "<< i << std::endl;
          //auto hmsg = std::get<0>(stat->cache[i][k]);
          //auto msg = std::get<1>(stat->cache[i][k]);
          auto msg = stat->cache[i][k];
          //printf("hmsg %x, msg %x\n", hmsg, msg);
          printf("msg %x\n", msg);
          //zmq_msg_send(hmsg, stat->responder, ZMQ_SNDMORE);
          int flag = ZMQ_SNDMORE;
          if (i == NUM_STREAMS-1) {
            flag = 0;
          }

          stat->cache[i].erase(k);

          printf("msg after erase %x\n", msg);
          
          zmq_msg_send(msg, stat->responder, flag);
          done = k;
          //printf("after hmsg %x, msg %x\n", hmsg, msg);
          //free ( stat->cache[i][k]);
          //
        }
      }
    }
  }
}

/**
 * Start Acquisition Call back (slsMultiReceiver writes data if file write
 * enabled) if registerCallBackRawDataReady or
 * registerCallBackRawDataModifyReady registered, users get data
 */
int StartAcq(const std::string &filePath, const std::string &fileName,
             uint64_t fileIndex, size_t imageSize, void *objectPointer) {
    std::cout << "#### StartAcq:  filePath:" << filePath
                          << "  fileName:" << fileName
                          << " fileIndex:" << fileIndex
                          << "  imageSize:" << imageSize << " ####" << std::endl;
    Status* stat = static_cast<Status*>(objectPointer);
    std::ostringstream oss;
    oss << "{\"htype\":\"header\""
        << ", \"msg_number\":" << stat->msg_number
        << ", \"filename\":\"" << filePath << fileName
        << "\"}\n";
    
    std::string message = oss.str();
    int length = message.length();
    try{
      {
        std::lock_guard<std::mutex> lock(stat->mtx);
        //zmq_send(stat->responder, message.c_str(), length, ZMQ_DONTWAIT);
        stat->msg_number++;
      }
    } catch (...) {
      // pass
    }

    return 0;
}

/** Acquisition Finished Call back */
void AcquisitionFinished(uint64_t framesCaught, void *objectPointer) {
    std::cout << "#### AcquisitionFinished: framesCaught:"
                          << framesCaught << " ####" << std::endl;

    Status* stat = static_cast<Status*>(objectPointer);
    std::ostringstream oss;
    oss << "{\"htype\":\"series_end\""
        << ", \"msg_number\":" << stat->msg_number
        << "\"}\n";
    
    std::string message = oss.str();
    int length = message.length();
    try{
      {
        std::lock_guard<std::mutex> lock(stat->mtx);
        //zmq_send(stat->responder, message.c_str(), length, ZMQ_DONTWAIT);
        stat->msg_number++;
      }
    } catch (...) {
      // pass
    }
}

/**
 * Get Receiver Data Call back
 * Prints in different colors(for each receiver process) the different headers
 * for each image call back.
 */
void GetData(slsDetectorDefs::sls_receiver_header &header, char *dataPointer,
             size_t imageSize, void *objectPointer) {
    slsDetectorDefs::sls_detector_header detectorHeader = header.detHeader;

    printf(
        "#### %d %d GetData: ####\n"
        "frameNumber: %lu\t\texpLength: %u\t\tpacketNumber: %u\t\tdetSpec1: %lu"
        "\t\ttimestamp: %lu\t\tmodId: %u\t\t"
        "row: %u\t\tcolumn: %u\t\tdetSpec2: %u\t\tdetSpec3: %u"
        "\t\tdetSpec4: %u\t\tdetType: %u\t\tversion: %u"
        //"\t\tpacketsMask:%s"
        "\t\tfirstbytedata: 0x%x\t\tdatsize: %zu\n\n",
        detectorHeader.column, detectorHeader.row,
        (long unsigned int)detectorHeader.frameNumber, detectorHeader.expLength,
        detectorHeader.packetNumber, (long unsigned int)detectorHeader.detSpec1,
        (long unsigned int)detectorHeader.timestamp, detectorHeader.modId,
        detectorHeader.row, detectorHeader.column, detectorHeader.detSpec2,
        detectorHeader.detSpec3, detectorHeader.detSpec4,
        detectorHeader.detType, detectorHeader.version,
        // header->packetsMask.to_string().c_str(),
        ((uint8_t)(*((uint8_t *)(dataPointer)))), imageSize);
    
    Status* stat = static_cast<Status*>(objectPointer);

    std::ostringstream oss;
    oss << "{\"htype\":\"module\""
        << ", \"column\":" << detectorHeader.column
        << ", \"row\":" << detectorHeader.row
        << "\"}\n";
    
    std::string message = oss.str();
    int length = message.length();

    std::cout << detectorHeader.column << ":creating json part" << std::endl;    
    zmq_msg_t hmsg;

    zmq_msg_init_data (&hmsg, &message, length, my_free, NULL);

    std::cout << detectorHeader.column << "created header frame" << std::endl;
    //zmq_msg_init_buffer (&hmsg, message, length);

    char* data = new char[imageSize];

    std::cout << detectorHeader.column << "allocated new buffer" << std::endl;

    //printf("data pointer %x, data %x\n", dataPointer, )
    memcpy(data, dataPointer, imageSize);

    std::cout << detectorHeader.column << "copied buffer" << std::endl;
    zmq_msg_t *msg = new zmq_msg_t;
    zmq_msg_init_data (msg, &data, imageSize, my_free, NULL);

    std::cout << detectorHeader.column << "copied data to data frame" << std::endl;
    //zmq_msg_init_buffer (&msg, dataPointer, imageSize);
    
    //std::tuple<zmq_msg_t *, zmq_msg_t *> msgTuple(&hmsg, &msg);

    {
      std::cout << detectorHeader.column << "getting lock" << std::endl;
      std::lock_guard<std::mutex> lock(stat->mtx);
      //stat->cache[0][(long unsigned int)42] = nullptr;
      std::cout << detectorHeader.column << "put data in cache" << std::endl;
      stat->cache[detectorHeader.column][(long unsigned int)detectorHeader.frameNumber] = msg;
    }
    std::cout << detectorHeader.column << "call not correlate" << std::endl;
    sem_post(&stat->available);
}

/**
 * Get Receiver Data Call back (modified)
 * Prints in different colors(for each receiver process) the different headers
 * for each image call back.
 * @param modifiedImageSize new data size in bytes after the callback.
 * This will be the size written/streamed. (only smaller value is allowed).
 */
void GetData(slsDetectorDefs::sls_receiver_header &header, char *dataPointer,
             size_t &modifiedImageSize, void *objectPointer) {
    slsDetectorDefs::sls_detector_header detectorHeader = header.detHeader;

    printf(
        "#### %d %d GetData: ####\n"
        "frameNumber: %lu\t\texpLength: %u\t\tpacketNumber: %u\t\tdetSpec1: %lu"
        "\t\ttimestamp: %lu\t\tmodId: %u\t\t"
        "row: %u\t\tcolumn: %u\t\tdetSpec2: %u\t\tdetSpec3: %u"
        "\t\tdetSpec4: %u\t\tdetType: %u\t\tversion: %u"
        //"\t\tpacketsMask:%s"
        "\t\tfirstbytedata: 0x%x\t\tdatsize: %zu\n\n",
        detectorHeader.column, detectorHeader.row,
        (long unsigned int)detectorHeader.frameNumber, detectorHeader.expLength,
        detectorHeader.packetNumber, (long unsigned int)detectorHeader.detSpec1,
        (long unsigned int)detectorHeader.timestamp, detectorHeader.modId,
        detectorHeader.row, detectorHeader.column, detectorHeader.detSpec2,
        detectorHeader.detSpec3, detectorHeader.detSpec4,
        detectorHeader.detType, detectorHeader.version,
        // header->packetsMask.to_string().c_str(),
        *reinterpret_cast<uint8_t *>(dataPointer), modifiedImageSize);

    // if data is modified, eg ROI and size is reduced
    modifiedImageSize = 26000;
}


int main(int argc, char *argv[]) {

  sem_init(&semaphore, 1, 0);

  std::cout << "Created [ Tid: " << syscall(SYS_gettid) << " ]";

  // Catch signal SIGINT to close files and call destructors properly
  struct sigaction sa;
  sa.sa_flags = 0;                     // no flags
  sa.sa_handler = sigInterruptHandler; // handler function
  sigemptyset(&sa.sa_mask); // dont block additional signals during invocation
                            // of handler
  if (sigaction(SIGINT, &sa, nullptr) == -1) {
    std::cout << "Could not set handler function for SIGINT";
  }

  // if socket crash, ignores SISPIPE, prevents global signal handler
  // subsequent read/write to socket gives error - must handle locally
  struct sigaction asa;
  asa.sa_flags = 0;          // no flags
  asa.sa_handler = SIG_IGN;  // handler function
  sigemptyset(&asa.sa_mask); // dont block additional signals during
                             // invocation of handler
  if (sigaction(SIGPIPE, &asa, nullptr) == -1) {
    std::cout << "Could not set handler function for SIGPIPE";
  }

  try {
    sls::Receiver r(argc, argv);

    void *context = zmq_ctx_new ();

    void *responder = zmq_socket (context, ZMQ_PUSH);
    int rc = zmq_bind (responder, "tcp://*:5555");
    if (rc != 0){
      std::cout << "failed to bind";
    }

    
    Status stat{responder, 0};

    sem_init(&stat.available, 0, 0);

    
    void* user_data = static_cast<void *>(&stat);

    std::thread combinerThread(Correlate, &stat);
    
    // register call backs
    /** - Call back for start acquisition */
    std::cout << "Registering 	StartAcq()" << std::endl;
    r.registerCallBackStartAcquisition(StartAcq, user_data);

    /** - Call back for acquisition finished */
    std::cout << "Registering 	AcquisitionFinished()" << std::endl;
    r.registerCallBackAcquisitionFinished(AcquisitionFinished, user_data);

    /* 	- Call back for raw data */
    std::cout << "Registering GetData()" << std::endl;
    r.registerCallBackRawDataReady(GetData, user_data);

    std::cout << "[ Press \'Ctrl+c\' to exit ]";
    sem_wait(&semaphore);
    sem_destroy(&semaphore);

    std::cout << "set terminate" << std::endl;
    stat.terminate = true;
    sem_post(&stat.available);
    combinerThread.join();
    sem_destroy(&stat.available);

    zmq_ctx_destroy(context);
  } catch (...) {
    // pass
  }
  std::cout << "Exiting [ Tid: " << syscall(SYS_gettid) << " ]";
  std::cout << "Exiting Receiver";
  return 0;
}