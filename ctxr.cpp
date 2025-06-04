#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <filesystem>
#include <algorithm>
#include <iomanip>
#include <sstream>
#include <chrono>
#include <cstring>
#include <memory>
#include <array>
#include <regex>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>

// For decompression
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/zlib.hpp>

// For HTTP streaming
#include <curl/curl.h>

namespace fs = std::filesystem;

// Constants from rippled
static constexpr uint32_t CATL = 0x4C544143UL; // "CATL" in LE
static constexpr uint16_t CATALOGUE_VERSION_MASK = 0x00FF;
static constexpr uint16_t CATALOGUE_COMPRESS_LEVEL_MASK = 0x0F00;

// SHAMap node types
enum SHAMapNodeType : uint8_t {
    tnINNER = 1,
    tnTRANSACTION_NM = 2,  // transaction, no metadata
    tnTRANSACTION_MD = 3,  // transaction, with metadata
    tnACCOUNT_STATE = 4,
    tnREMOVE = 254,
    tnTERMINAL = 255
};

#pragma pack(push, 1)
struct CATLHeader {
    uint32_t magic;
    uint32_t min_ledger;
    uint32_t max_ledger;
    uint16_t version;
    uint16_t network_id;
    uint64_t filesize = 0;
    std::array<uint8_t, 64> hash = {};
};
#pragma pack(pop)

// Custom streambuf for CURL that provides buffering
class CurlStreamBuf : public std::streambuf {
private:
    CURL* curl_;
    std::string url_;
    std::atomic<bool> downloading_{true};
    std::atomic<bool> error_{false};
    std::thread downloadThread_;
    
    // Circular buffer
    static constexpr size_t BUFFER_SIZE = 1024 * 1024; // 1MB buffer
    std::vector<char> buffer_;
    std::atomic<size_t> writePos_{0};
    std::atomic<size_t> readPos_{0};
    std::mutex mutex_;
    std::condition_variable dataAvailable_;
    std::condition_variable spaceAvailable_;
    
    // Statistics
    std::atomic<size_t> totalBytesReceived_{0};
    std::atomic<double> downloadSpeed_{0.0};
    
    // Internal read buffer for streambuf
    static constexpr size_t READ_BUFFER_SIZE = 8192;
    std::vector<char> readBuffer_;
    
    static size_t writeCallback(char* ptr, size_t size, size_t nmemb, void* userdata) {
        CurlStreamBuf* buf = static_cast<CurlStreamBuf*>(userdata);
        size_t totalSize = size * nmemb;
        return buf->writeData(ptr, totalSize);
    }
    
    static int progressCallback(void* userdata, curl_off_t dltotal, curl_off_t dlnow, 
                               curl_off_t ultotal, curl_off_t ulnow) {
        (void)ultotal;
        (void)ulnow;
        (void)dltotal;
        
        CurlStreamBuf* buf = static_cast<CurlStreamBuf*>(userdata);
        buf->totalBytesReceived_ = dlnow;
        
        // Get download speed
        double speed;
        curl_easy_getinfo(buf->curl_, CURLINFO_SPEED_DOWNLOAD, &speed);
        buf->downloadSpeed_ = speed;
        
        return 0;
    }
    
    size_t writeData(const char* data, size_t size) {
        std::unique_lock<std::mutex> lock(mutex_);
        
        size_t written = 0;
        while (written < size && downloading_) {
            size_t writeP = writePos_.load();
            size_t readP = readPos_.load();
            
            // Calculate available space
            size_t available = (readP + BUFFER_SIZE - writeP - 1) % BUFFER_SIZE;
            
            if (available == 0) {
                // Buffer full, wait for space
                spaceAvailable_.wait_for(lock, std::chrono::milliseconds(100));
                continue;
            }
            
            // Write as much as we can
            size_t toWrite = std::min(size - written, available);
            size_t endPos = (writeP + toWrite) % BUFFER_SIZE;
            
            if (endPos > writeP) {
                // Simple copy
                std::memcpy(&buffer_[writeP], data + written, toWrite);
            } else {
                // Wrap around
                size_t firstPart = BUFFER_SIZE - writeP;
                std::memcpy(&buffer_[writeP], data + written, firstPart);
                std::memcpy(&buffer_[0], data + written + firstPart, toWrite - firstPart);
            }
            
            writePos_ = endPos;
            written += toWrite;
            dataAvailable_.notify_one();
        }
        
        return written;
    }
    
    void downloadThread() {
        CURLcode res = curl_easy_perform(curl_);
        if (res != CURLE_OK) {
            std::cerr << "\nCURL error: " << curl_easy_strerror(res) << std::endl;
            error_ = true;
        }
        downloading_ = false;
        dataAvailable_.notify_all(); // Wake up any waiting readers
    }
    
    // Override streambuf's underflow to refill buffer
    virtual int_type underflow() override {
        if (gptr() < egptr()) {
            return traits_type::to_int_type(*gptr());
        }
        
        // Try to read more data
        std::unique_lock<std::mutex> lock(mutex_);
        
        size_t bytesRead = 0;
        while (bytesRead == 0) {
            size_t writeP = writePos_.load();
            size_t readP = readPos_.load();
            
            // Calculate available data
            size_t available = (writeP + BUFFER_SIZE - readP) % BUFFER_SIZE;
            
            if (available == 0) {
                if (!downloading_ || error_) {
                    // No more data
                    return traits_type::eof();
                }
                // Wait for data
                dataAvailable_.wait_for(lock, std::chrono::milliseconds(100));
                continue;
            }
            
            // Read as much as we can into our buffer
            size_t toRead = std::min(available, READ_BUFFER_SIZE);
            size_t endPos = (readP + toRead) % BUFFER_SIZE;
            
            if (endPos > readP) {
                // Simple copy
                std::memcpy(readBuffer_.data(), &buffer_[readP], toRead);
            } else {
                // Wrap around
                size_t firstPart = BUFFER_SIZE - readP;
                std::memcpy(readBuffer_.data(), &buffer_[readP], firstPart);
                std::memcpy(readBuffer_.data() + firstPart, &buffer_[0], toRead - firstPart);
            }
            
            readPos_ = endPos;
            bytesRead = toRead;
            spaceAvailable_.notify_one();
        }
        
        // Set buffer pointers
        setg(readBuffer_.data(), readBuffer_.data(), readBuffer_.data() + bytesRead);
        
        return traits_type::to_int_type(*gptr());
    }
    
public:
    CurlStreamBuf(const std::string& url) : url_(url), buffer_(BUFFER_SIZE), readBuffer_(READ_BUFFER_SIZE) {
        curl_ = curl_easy_init();
        if (!curl_) {
            throw std::runtime_error("Failed to initialize CURL");
        }
        
        // Set up CURL options
        curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, writeCallback);
        curl_easy_setopt(curl_, CURLOPT_WRITEDATA, this);
        curl_easy_setopt(curl_, CURLOPT_XFERINFOFUNCTION, progressCallback);
        curl_easy_setopt(curl_, CURLOPT_XFERINFODATA, this);
        curl_easy_setopt(curl_, CURLOPT_NOPROGRESS, 0L);
        curl_easy_setopt(curl_, CURLOPT_FOLLOWLOCATION, 1L);
        curl_easy_setopt(curl_, CURLOPT_FAILONERROR, 1L);
        curl_easy_setopt(curl_, CURLOPT_BUFFERSIZE, 120000L);
        
        // Initialize streambuf
        setg(readBuffer_.data(), readBuffer_.data(), readBuffer_.data());
        
        // Start download thread
        downloadThread_ = std::thread(&CurlStreamBuf::downloadThread, this);
    }
    
    ~CurlStreamBuf() {
        downloading_ = false;
        if (downloadThread_.joinable()) {
            downloadThread_.join();
        }
        if (curl_) {
            curl_easy_cleanup(curl_);
        }
    }
    
    size_t getTotalBytesReceived() const { return totalBytesReceived_; }
    double getDownloadSpeed() const { return downloadSpeed_; }
    bool hasError() const { return error_; }
};

// Simple istream wrapper for CurlStreamBuf
class CurlStream : public std::istream {
private:
    std::unique_ptr<CurlStreamBuf> buf_;
    
public:
    CurlStream(const std::string& url) 
        : std::istream(nullptr), buf_(std::make_unique<CurlStreamBuf>(url)) {
        rdbuf(buf_.get());
    }
    
    size_t getTotalBytesReceived() const { return buf_->getTotalBytesReceived(); }
    double getDownloadSpeed() const { return buf_->getDownloadSpeed(); }
    bool hasError() const { return buf_->hasError(); }
};

class TransactionExtractor {
private:
    std::string source_;
    bool isHttpSource_;
    CATLHeader header_;
    uint8_t compressionLevel_ = 0;
    uint16_t networkId_;
    
    // Statistics
    size_t totalTransactions_ = 0;
    size_t skippedTransactions_ = 0;
    std::chrono::steady_clock::time_point startTime_;
    
    // Resume state
    uint32_t resumeLedger_ = 0;
    uint32_t resumeTxIndex_ = 0;
    bool resumeMode_ = false;
    
    // For HTTP sources
    std::unique_ptr<CurlStream> curlStream_;
    
    // Helper functions
    inline uint8_t getCatalogueVersion(uint16_t versionField) {
        return versionField & CATALOGUE_VERSION_MASK;
    }
    
    inline uint8_t getCompressionLevel(uint16_t versionField) {
        return (versionField & CATALOGUE_COMPRESS_LEVEL_MASK) >> 8;
    }
    
    std::string constructCTID(uint32_t ledger, uint32_t txIndex) {
        std::stringstream ss;
        ss << "C" 
           << std::uppercase << std::hex << std::setfill('0')
           << std::setw(7) << ledger
           << std::setw(4) << txIndex
           << std::setw(4) << networkId_;
        return ss.str();
    }
    
    std::pair<uint32_t, uint32_t> parseCTID(const std::string& ctid) {
        if (ctid.length() != 16 || ctid[0] != 'C') {
            return {0, 0};
        }
        
        uint32_t ledger = std::stoul(ctid.substr(1, 7), nullptr, 16);
        uint32_t txIndex = std::stoul(ctid.substr(8, 4), nullptr, 16);
        return {ledger, txIndex};
    }
    
    void displayProgress(uint32_t currentLedger = 0) {
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - startTime_).count();
        
        std::cout << "\rTx extracted: " << totalTransactions_ 
                  << " | Skipped: " << skippedTransactions_;
        
        if (currentLedger > 0) {
            std::cout << " | Ledger: " << currentLedger;
        }
        
        if (isHttpSource_ && curlStream_) {
            double speed = curlStream_->getDownloadSpeed();
            size_t received = curlStream_->getTotalBytesReceived();
            std::cout << " | Downloaded: " << (received / 1024 / 1024) << "MB";
            if (speed > 0) {
                std::cout << " @ " << std::fixed << std::setprecision(1) 
                         << (speed / 1024 / 1024) << "MB/s";
            }
        }
        
        std::cout << " | Time: " << elapsed << "s" << std::flush;
    }
    
    bool skipBytes(std::istream& stream, size_t count) {
        // For large skips, read in chunks
        const size_t bufferSize = 8192;
        std::vector<char> buffer(std::min(count, bufferSize));
        
        while (count > 0) {
            size_t toRead = std::min(count, bufferSize);
            stream.read(buffer.data(), toRead);
            if (stream.gcount() < static_cast<std::streamsize>(toRead)) {
                return false;
            }
            count -= toRead;
        }
        return true;
    }
    
    bool skipLedgerInfo(std::istream& stream) {
        // Skip ledger info fields:
        // - hash (32 bytes)
        // - txHash (32 bytes)
        // - accountHash (32 bytes)
        // - parentHash (32 bytes)
        // - drops (8 bytes)
        // - closeFlags (4 bytes)
        // - closeTimeResolution (4 bytes)
        // - closeTime (8 bytes)
        // - parentCloseTime (8 bytes)
        const size_t ledgerInfoSize = 32 + 32 + 32 + 32 + 8 + 4 + 4 + 8 + 8;
        return skipBytes(stream, ledgerInfoSize);
    }
    
    bool skipSHAMap(std::istream& stream) {
        while (!stream.eof()) {
            uint8_t nodeType;
            stream.read(reinterpret_cast<char*>(&nodeType), 1);
            
            if (stream.gcount() < 1) return false;
            
            if (nodeType == tnTERMINAL) {
                return true; // Successfully reached end of map
            }
            
            // Skip key (32 bytes)
            if (!skipBytes(stream, 32)) return false;
            
            // For tnREMOVE, we're done with this node
            if (nodeType == tnREMOVE) continue;
            
            // Read data size
            uint32_t dataSize;
            stream.read(reinterpret_cast<char*>(&dataSize), 4);
            if (stream.gcount() < 4) return false;
            
            // Sanity check
            if (dataSize > 10 * 1024 * 1024) {
                std::cerr << "\nWARNING: Suspicious data size in SHAMap: " << dataSize << std::endl;
                return false;
            }
            
            // Skip data
            if (!skipBytes(stream, dataSize)) return false;
        }
        
        return false; // Should have found terminal
    }
    
    bool extractTransactionsFromMap(std::istream& stream, uint32_t ledgerSeq) {
        uint32_t txIndex = 0;
        
        // If resuming this ledger, skip transactions we've already extracted
        if (resumeMode_ && ledgerSeq == resumeLedger_) {
            txIndex = resumeTxIndex_;
        }
        
        uint32_t currentTxIndex = 0;
        
        while (!stream.eof()) {
            uint8_t nodeType;
            stream.read(reinterpret_cast<char*>(&nodeType), 1);
            
            if (stream.gcount() < 1) return false;
            
            if (nodeType == tnTERMINAL) {
                return true; // Successfully reached end of map
            }
            
            // Read key (32 bytes) - this is the transaction hash
            std::array<uint8_t, 32> txHash;
            stream.read(reinterpret_cast<char*>(txHash.data()), 32);
            if (stream.gcount() < 32) return false;
            
            // For tnREMOVE, we're done with this node
            if (nodeType == tnREMOVE) continue;
            
            // Read data size
            uint32_t dataSize;
            stream.read(reinterpret_cast<char*>(&dataSize), 4);
            if (stream.gcount() < 4) return false;
            
            // Sanity check
            if (dataSize > 10 * 1024 * 1024) {
                std::cerr << "\nWARNING: Suspicious transaction size: " << dataSize << std::endl;
                return false;
            }
            
            // Check if this is a transaction node
            if (nodeType == tnTRANSACTION_NM || nodeType == tnTRANSACTION_MD) {
                // Skip if we're resuming and haven't reached our resume point yet
                if (resumeMode_ && ledgerSeq == resumeLedger_ && currentTxIndex < resumeTxIndex_) {
                    if (!skipBytes(stream, dataSize)) return false;
                    currentTxIndex++;
                    continue;
                }
                
                // Construct CTID
                std::string ctid = constructCTID(ledgerSeq, txIndex);
                std::string filename = ctid + ".hex";
                
                // Check if file already exists
                if (fs::exists(filename)) {
                    skippedTransactions_++;
                    if (!skipBytes(stream, dataSize)) return false;
                } else {
                    // Read transaction data
                    std::vector<uint8_t> txData(dataSize);
                    stream.read(reinterpret_cast<char*>(txData.data()), dataSize);
                    if (stream.gcount() < static_cast<std::streamsize>(dataSize)) return false;
                    
                    // Save to file in hex format
                    std::ofstream outFile(filename);
                    if (outFile.is_open()) {
                        for (uint8_t byte : txData) {
                            outFile << std::hex << std::setw(2) << std::setfill('0') 
                                   << static_cast<int>(byte);
                        }
                        outFile.close();
                        totalTransactions_++;
                    } else {
                        std::cerr << "\nERROR: Failed to create file: " << filename << std::endl;
                    }
                }
                
                txIndex++;
                currentTxIndex++;
                
                // Update progress every 100 transactions
                if ((totalTransactions_ + skippedTransactions_) % 100 == 0) {
                    displayProgress(ledgerSeq);
                }
                
                // Clear resume mode after processing first transaction
                if (resumeMode_ && ledgerSeq == resumeLedger_) {
                    resumeMode_ = false;
                }
            } else {
                // Not a transaction, skip the data
                if (!skipBytes(stream, dataSize)) return false;
            }
        }
        
        return false; // Should have found terminal
    }
    
    void checkForResume() {
        std::cout << "Checking for existing CTID files..." << std::endl;
        
        // Pattern to match CTID files for this network
        std::stringstream pattern;
        pattern << "C[0-9A-F]{7}[0-9A-F]{4}" 
                << std::hex << std::uppercase << std::setfill('0') << std::setw(4) 
                << networkId_ << "\\.hex";
        std::regex ctidRegex(pattern.str());
        
        uint32_t maxLedger = 0;
        uint32_t maxTxIndex = 0;
        std::string latestFile;
        
        for (const auto& entry : fs::directory_iterator(fs::current_path())) {
            if (entry.is_regular_file()) {
                std::string filename = entry.path().filename().string();
                if (std::regex_match(filename, ctidRegex)) {
                    std::string ctid = filename.substr(0, 16);
                    auto [ledger, txIndex] = parseCTID(ctid);
                    
                    if (ledger > maxLedger || (ledger == maxLedger && txIndex > maxTxIndex)) {
                        maxLedger = ledger;
                        maxTxIndex = txIndex;
                        latestFile = filename;
                    }
                    
                    skippedTransactions_++;
                }
            }
        }
        
        if (!latestFile.empty()) {
            std::cout << "Found " << skippedTransactions_ << " existing transaction files." << std::endl;
            std::cout << "Latest: " << latestFile << " (Ledger " << maxLedger 
                     << ", Tx " << maxTxIndex << ")" << std::endl;
            
            std::cout << "Resume from next transaction? (y/n): ";
            std::string answer;
            std::getline(std::cin, answer);
            
            if (answer == "y" || answer == "Y") {
                resumeLedger_ = maxLedger;
                resumeTxIndex_ = maxTxIndex + 1;
                resumeMode_ = true;
                std::cout << "Resuming from Ledger " << resumeLedger_ 
                         << ", Transaction " << resumeTxIndex_ << std::endl;
            } else {
                skippedTransactions_ = 0; // Reset count if not resuming
            }
        } else {
            std::cout << "No existing CTID files found. Starting from the beginning." << std::endl;
        }
    }
    
public:
    TransactionExtractor(const std::string& source) 
        : source_(source) {
        startTime_ = std::chrono::steady_clock::now();
        
        // Check if source is HTTP(S)
        isHttpSource_ = (source.substr(0, 7) == "http://" || source.substr(0, 8) == "https://");
    }
    
    bool extract() {
        std::istream* inputStream = nullptr;
        std::unique_ptr<std::ifstream> fileStream;
        std::unique_ptr<boost::iostreams::filtering_istream> dataStream;
        
        if (isHttpSource_) {
            std::cout << "Streaming from HTTP source: " << source_ << std::endl;
            
            // Initialize CURL globally (only once)
            static bool curlInitialized = false;
            if (!curlInitialized) {
                curl_global_init(CURL_GLOBAL_DEFAULT);
                curlInitialized = true;
            }
            
            // Create CURL stream
            try {
                curlStream_ = std::make_unique<CurlStream>(source_);
                inputStream = curlStream_.get();
            } catch (const std::exception& e) {
                std::cerr << "ERROR: Failed to initialize HTTP stream: " << e.what() << std::endl;
                return false;
            }
        } else {
            // File source
            fileStream = std::make_unique<std::ifstream>(source_, std::ios::binary);
            if (!fileStream->is_open()) {
                std::cerr << "ERROR: Failed to open file: " << source_ << std::endl;
                return false;
            }
            inputStream = fileStream.get();
        }
        
        // Read header
        inputStream->read(reinterpret_cast<char*>(&header_), sizeof(CATLHeader));
        if (inputStream->gcount() < static_cast<std::streamsize>(sizeof(CATLHeader))) {
            std::cerr << "ERROR: Failed to read header" << std::endl;
            return false;
        }
        
        // Validate header
        if (header_.magic != CATL) {
            std::cerr << "ERROR: Invalid CATL magic number" << std::endl;
            return false;
        }
        
        networkId_ = header_.network_id;
        compressionLevel_ = getCompressionLevel(header_.version);
        
        std::cout << "=== CATL Transaction Extractor ===" << std::endl;
        std::cout << "Source: " << source_ << std::endl;
        std::cout << "Network ID: " << std::hex << networkId_ << std::dec << std::endl;
        std::cout << "Ledger range: " << header_.min_ledger << " - " << header_.max_ledger << std::endl;
        std::cout << "Compression level: " << static_cast<int>(compressionLevel_) << std::endl;
        if (!isHttpSource_ && header_.filesize > 0) {
            std::cout << "File size: " << (header_.filesize / 1024 / 1024) << " MB" << std::endl;
        }
        std::cout << std::endl;
        
        // Check for existing files to resume
        checkForResume();
        std::cout << std::endl;
        
        // Set up decompression stream
        dataStream = std::make_unique<boost::iostreams::filtering_istream>();
        
        if (compressionLevel_ > 0) {
            boost::iostreams::zlib_params params;
            params.window_bits = 15;
            params.noheader = false;
            dataStream->push(boost::iostreams::zlib_decompressor(params));
        }
        dataStream->push(*inputStream);
        
        // Process ledgers
        uint32_t currentLedger = header_.min_ledger;
        uint32_t ledgersProcessed = 0;
        
        // Skip to resume ledger if needed
        if (resumeMode_ && resumeLedger_ > header_.min_ledger) {
            std::cout << "Skipping to ledger " << resumeLedger_ << "..." << std::endl;
            
            while (currentLedger < resumeLedger_ && !dataStream->eof()) {
                // Read ledger sequence
                uint32_t ledgerSeq;
                dataStream->read(reinterpret_cast<char*>(&ledgerSeq), 4);
                if (dataStream->gcount() < 4) break;
                
                // Skip ledger info
                if (!skipLedgerInfo(*dataStream)) break;
                
                // Skip state map
                if (!skipSHAMap(*dataStream)) break;
                
                // Skip transaction map
                if (!skipSHAMap(*dataStream)) break;
                
                currentLedger = ledgerSeq + 1;
                
                if (currentLedger % 100 == 0) {
                    std::cout << "\rSkipping... Current ledger: " << currentLedger << std::flush;
                }
            }
            std::cout << std::endl;
        }
        
        // Extract transactions from remaining ledgers
        while (!dataStream->eof()) {
            // Check for HTTP errors
            if (isHttpSource_ && curlStream_ && curlStream_->hasError()) {
                std::cerr << "\nERROR: HTTP stream error detected" << std::endl;
                break;
            }
            
            // Read ledger sequence
            uint32_t ledgerSeq;
            dataStream->read(reinterpret_cast<char*>(&ledgerSeq), 4);
            if (dataStream->gcount() < 4) break;
            
            // Sanity check
            if (ledgerSeq < header_.min_ledger || ledgerSeq > header_.max_ledger) {
                std::cerr << "\nWARNING: Unexpected ledger sequence: " << ledgerSeq << std::endl;
                break;
            }
            
            // Skip ledger info
            if (!skipLedgerInfo(*dataStream)) {
                std::cerr << "\nERROR: Failed to skip ledger info for ledger " << ledgerSeq << std::endl;
                break;
            }
            
            // Skip state map (first ledger has full state, others have deltas)
            if (!skipSHAMap(*dataStream)) {
                std::cerr << "\nERROR: Failed to skip state map for ledger " << ledgerSeq << std::endl;
                break;
            }
            
            // Extract transactions from transaction map
            if (!extractTransactionsFromMap(*dataStream, ledgerSeq)) {
                std::cerr << "\nERROR: Failed to process transaction map for ledger " << ledgerSeq << std::endl;
                break;
            }
            
            currentLedger = ledgerSeq;
            ledgersProcessed++;
            
            // Progress checkpoint every 1000 ledgers
            if (ledgersProcessed % 1000 == 0) {
                std::cout << "\nCheckpoint: Processed " << ledgersProcessed 
                         << " ledgers (up to " << currentLedger << ")" << std::endl;
                displayProgress(currentLedger);
                std::cout << std::endl;
            }
        }
        
        // Final summary
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - startTime_).count();
        
        std::cout << "\n\n=== Extraction Complete ===" << std::endl;
        std::cout << "Total transactions extracted: " << totalTransactions_ << std::endl;
        std::cout << "Transactions skipped (already exist): " << skippedTransactions_ << std::endl;
        std::cout << "Ledgers processed: " << ledgersProcessed << std::endl;
        std::cout << "Last ledger: " << currentLedger << std::endl;
        std::cout << "Total time: " << elapsed << " seconds" << std::endl;
        
        if (isHttpSource_ && curlStream_) {
            size_t totalMB = curlStream_->getTotalBytesReceived() / 1024 / 1024;
            std::cout << "Total downloaded: " << totalMB << " MB" << std::endl;
            if (elapsed > 0) {
                std::cout << "Average download speed: " << std::fixed << std::setprecision(1)
                         << (static_cast<double>(totalMB) / elapsed) << " MB/s" << std::endl;
            }
        }
        
        if (totalTransactions_ > 0 && elapsed > 0) {
            std::cout << "Average extraction rate: " << std::fixed << std::setprecision(1) 
                     << (static_cast<double>(totalTransactions_) / elapsed) 
                     << " transactions/second" << std::endl;
        }
        
        return true;
    }
};

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <source> [source2] ..." << std::endl;
        std::cerr << "\nExtracts transactions from XRPL catalogue files." << std::endl;
        std::cerr << "Sources can be local files or HTTP(S) URLs." << std::endl;
        std::cerr << "\nExamples:" << std::endl;
        std::cerr << "  " << argv[0] << " catalogue.dat" << std::endl;
        std::cerr << "  " << argv[0] << " https://example.com/catalogue.dat" << std::endl;
        std::cerr << "  " << argv[0] << " file1.dat https://example.com/file2.dat file3.dat" << std::endl;
        std::cerr << "\nSupports automatic resume functionality and streaming from HTTP sources." << std::endl;
        return 1;
    }
    
    // Process each source
    for (int i = 1; i < argc; i++) {
        std::cout << "\n";
        if (i > 1) {
            std::cout << "=====================================\n";
        }
        
        TransactionExtractor extractor(argv[i]);
        if (!extractor.extract()) {
            std::cerr << "Failed to process: " << argv[i] << std::endl;
            continue;
        }
    }
    
    return 0;
}
