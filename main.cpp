#include <bits/stdc++.h>
#include <thread>
#include <mutex>
#include <chrono>
#include <filesystem>
#include <iomanip>
using namespace std;
namespace fs = filesystem;

// ---------- CONFIGURATION ----------
int NUM_DATANODES = 3;
const int REPLICATION_FACTOR = 2;
const int BLOCK_SIZE = 4;
const int HEARTBEAT_INTERVAL_MS = 50000;

// ---------- COLOR CODES ----------
#define RESET   "\033[0m"
#define RED     "\033[31m"
#define GREEN   "\033[32m"
#define YELLOW  "\033[33m"
#define BLUE    "\033[34m"
#define MAGENTA "\033[35m"
#define CYAN    "\033[36m"
#define BOLD    "\033[1m"

// ---------- STRUCTURES ----------
struct Block {
    string data;
    vector<int> dataNodeIDs;
};

struct FileMetadata {
    string filename;
    vector<Block> blocks;
};

// ---------- GLOBALS ----------
vector<vector<string>> dataNodes;
unordered_map<string, FileMetadata> nameNode;
vector<bool> nodeAlive;
mutex mtx;
bool heartbeatRunning = true;

// Storage directories
const string STORAGE_DIR = "storage";
const string METADATA_DIR = "metadata";

// ---------- UTILITY FUNCTIONS ----------
void createDirectories() {
    fs::create_directories(STORAGE_DIR);
    fs::create_directories(METADATA_DIR);
    for (int i = 0; i < NUM_DATANODES; ++i) {
        fs::create_directories(STORAGE_DIR + "/node_" + to_string(i));
    }
}

void printHeader(const string& text) {
    cout << CYAN << BOLD << "\n+===========================================================+" << RESET << endl;
    cout << CYAN << BOLD << "| " << setw(57) << left << text << " |" << RESET << endl;
    cout << CYAN << BOLD << "+===========================================================+" << RESET << endl;
}

void printHeader1(const string& text) {
    cout << MAGENTA << BOLD << "\n+===========================================================+" << RESET << endl;
    cout << MAGENTA << BOLD << "| " << setw(57) << left << text << " |" << RESET << endl;
    cout << MAGENTA << BOLD << "+===========================================================+" << RESET << endl;
}

void printSuccess(const string& text) {
    cout << GREEN << "[SUCCESS] " << text << RESET << endl;
}

void printError(const string& text) {
    cout << RED << "[ERROR] " << text << RESET << endl;
}

void printWarning(const string& text) {
    cout << YELLOW << "[WARNING] " << text << RESET << endl;
}

void printInfo(const string& text) {
    //cout << BLUE << "[INFO] " << text << RESET << endl;
}

// Safe node access functions
bool isNodeAlive(int id) {
    return id >= 0 && id < nodeAlive.size() && nodeAlive[id];
}

bool isValidNode(int id) {
    return id >= 0 && id < NUM_DATANODES && id < dataNodes.size() && id < nodeAlive.size();
}

// ---------- STORAGE FUNCTIONS ----------
void saveNodeData(int nodeId) {
    if (!isValidNode(nodeId)) return;
    
    ofstream file(STORAGE_DIR + "/node_" + to_string(nodeId) + "/blocks.txt");
    for (const auto& block : dataNodes[nodeId]) {
        file << block << endl;
    }
    file.close();
}

void saveAllNodeData() {
    for (int i = 0; i < NUM_DATANODES && i < dataNodes.size(); ++i) {
        saveNodeData(i);
    }
}

void saveMetadata() {
    ofstream file(METADATA_DIR + "/namenode_metadata.txt");
    for (const auto& entry : nameNode) {
        file << "File: " << entry.first << endl;
        for (int i = 0; i < entry.second.blocks.size(); ++i) {
            file << "  Block " << i << ": " << entry.second.blocks[i].data;
            file << " [Nodes: ";
            for (int nodeId : entry.second.blocks[i].dataNodeIDs) {
                file << nodeId << " ";
            }
            file << "]" << endl;
        }
        file << endl;
    }
    file.close();
}

void loadStorage() {
    // Load data from storage files if they exist
    for (int i = 0; i < NUM_DATANODES; ++i) {
        string filename = STORAGE_DIR + "/node_" + to_string(i) + "/blocks.txt";
        ifstream file(filename);
        if (file.is_open()) {
            string line;
            while (getline(file, line)) {
                if (!line.empty()) {
                    dataNodes[i].push_back(line);
                }
            }
            file.close();
        }
    }
}

// ---------- CORE FUNCTIONS ----------
vector<string> splitIntoBlocks(const string &content) {
    vector<string> blocks;
    for (int i = 0; i < (int)content.size(); i += BLOCK_SIZE)
        blocks.push_back(content.substr(i, BLOCK_SIZE));
    return blocks;
}

void uploadFile(const string &filename, const string &content) {
    vector<string> blocks = splitIntoBlocks(content);
    FileMetadata meta;
    meta.filename = filename;

    lock_guard<mutex> lock(mtx);

    for (auto &blkData : blocks) {
        Block block;
        block.data = blkData;

        unordered_set<int> used;
        while ((int)used.size() < REPLICATION_FACTOR) {
            int nodeID = rand() % NUM_DATANODES;
            if (used.insert(nodeID).second && isValidNode(nodeID)) {
                dataNodes[nodeID].push_back(blkData);
                block.dataNodeIDs.push_back(nodeID);
            }
        }
        meta.blocks.push_back(block);
    }

    nameNode[filename] = meta;
    saveAllNodeData();
    saveMetadata();
    printSuccess("File '" + filename + "' uploaded successfully.");
}

void listFiles() {
    lock_guard<mutex> lock(mtx);
    printHeader1("FILES IN NAMENODE");
    if (nameNode.empty()) {
        printInfo("No files stored");
        return;
    }
    
    cout << BOLD << MAGENTA << "| " << setw(30) << left << "Filename" 
         << " | " << setw(24) << "Blocks" << " |" << RESET << endl;
    cout << MAGENTA << "|--------------------------------|--------------------------|" << RESET << endl;
    
    for (auto &p : nameNode) {
        cout << "| " << setw(30) << left << p.first 
             << " | " << setw(24) << (to_string(p.second.blocks.size()) + " blocks") << " |" << endl;
    }
    cout << BOLD << MAGENTA << "|==========================================================|" << RESET << endl;
}

void showMetadata(const string &filename) {
    lock_guard<mutex> lock(mtx);
    if (!nameNode.count(filename)) {
        printError("File not found!");
        return;
    }
    
    auto &meta = nameNode[filename];
    printHeader("METADATA FOR: " + filename);
    
    cout << BOLD << CYAN << "| " << setw(6) << "Block" << " | " << setw(20) << "Stored on Nodes" 
         << " | " << setw(25) << "Data" << " |" << RESET << endl;
    cout << CYAN << "|--------|----------------------|---------------------------|" << RESET << endl;
    
    for (int i = 0; i < (int)meta.blocks.size(); ++i) {
        string nodes = "";
        for (int id : meta.blocks[i].dataNodeIDs) {
            if (isValidNode(id)) {
                nodes += to_string(id) + " ";
            }
        }
        cout << "| " << setw(6) << i << " | " << setw(20) << nodes 
             << " | " << setw(25) << meta.blocks[i].data << " |" << endl;
    }
}

void readFile(const string &filename) {
    lock_guard<mutex> lock(mtx);
    if (!nameNode.count(filename)) {
        printError("File not found!");
        return;
    }
    
    auto &meta = nameNode[filename];
    printHeader("READING FILE: " + filename);

    string result;
    int missingBlocks = 0;
    
    for (auto &block : meta.blocks) {
        bool found = false;
        for (int id : block.dataNodeIDs) {
            if (isValidNode(id) && isNodeAlive(id)) {
                result += block.data;
                found = true;
                break;
            }
        }
        if (!found) {
            printWarning("Block missing (all replicas failed): " + block.data);
            missingBlocks++;
        }
    }
    
    cout << "Reconstructed content: " << MAGENTA << "\"" << result << "\"" << RESET << endl;
    if (missingBlocks > 0) {
        printWarning(to_string(missingBlocks) + " blocks could not be recovered");
    }
}

// ---------- NODE OPERATIONS ----------
void killNode(int id) {
    lock_guard<mutex> lock(mtx);
    if (!isValidNode(id)) {
        printError("Invalid node ID!");
        return;
    }
    nodeAlive[id] = false;
    printWarning("DataNode " + to_string(id) + " has failed!");
    saveAllNodeData();
}

void recoverNode(int id) {
    lock_guard<mutex> lock(mtx);
    if (!isValidNode(id)) {
        printError("Invalid node ID!");
        return;
    }
    nodeAlive[id] = true;
    printSuccess("DataNode " + to_string(id) + " recovered.");
    saveAllNodeData();
}

// ---------- REPLICATION ----------
void replicateBlocks() {
    lock_guard<mutex> lock(mtx); // Ensure thread safety
    
    for (auto &file : nameNode) {
        for (auto &block : file.second.blocks) {
            // Remove invalid node references first
            block.dataNodeIDs.erase(
                remove_if(block.dataNodeIDs.begin(), block.dataNodeIDs.end(),
                    [&](int id) { return !isValidNode(id); }),
                block.dataNodeIDs.end()
            );

            int aliveCount = 0;
            for (int id : block.dataNodeIDs) {
                if (isNodeAlive(id)) aliveCount++;
            }

            if (aliveCount < REPLICATION_FACTOR) {
                for (int id = 0; id < NUM_DATANODES && aliveCount < REPLICATION_FACTOR; ++id) {
                    if (isValidNode(id) && isNodeAlive(id) && 
                        find(block.dataNodeIDs.begin(), block.dataNodeIDs.end(), id) == block.dataNodeIDs.end()) {
                        dataNodes[id].push_back(block.data);
                        block.dataNodeIDs.push_back(id);
                        aliveCount++;
                        printInfo("Block replicated to Node " + to_string(id) + ": " + block.data);
                    }
                }
            }
        }
    }
    saveAllNodeData();
    saveMetadata();
}

void heartbeat() {
    while (heartbeatRunning) {
        this_thread::sleep_for(chrono::milliseconds(HEARTBEAT_INTERVAL_MS));
        
        {
            lock_guard<mutex> lock(mtx);
            cout << YELLOW << "\n[HEARTBEAT] " << RESET;
            for (int i = 0; i < NUM_DATANODES && i < nodeAlive.size(); ++i) {
                cout << "Node" << i << "[" << (nodeAlive[i] ? GREEN "alive" : RED "dead") << RESET << "] ";
            }
            cout << endl;
        }
        
        replicateBlocks(); // replicateBlocks has its own lock
        cout.flush();
    }
}

void balanceNewNode(int newNodeId) {
    if (!isValidNode(newNodeId)) {
        printError("Invalid new node ID for balancing!");
        return;
    }

    printInfo("Starting load balancing to new Node " + to_string(newNodeId));

    int maxBlocks = 0, donorNode = -1;
    for (int i = 0; i < NUM_DATANODES - 1 && i < dataNodes.size(); ++i) {
        if (isValidNode(i) && (int)dataNodes[i].size() > maxBlocks) {
            maxBlocks = dataNodes[i].size();
            donorNode = i;
        }
    }

    if (donorNode != -1 && maxBlocks > 0 && isValidNode(donorNode)) {
        int toMove = maxBlocks / 2;
        for (int i = 0; i < toMove && !dataNodes[donorNode].empty(); ++i) {
            string blkData = dataNodes[donorNode].back();
            dataNodes[donorNode].pop_back();
            dataNodes[newNodeId].push_back(blkData);

            for (auto &file : nameNode) {
                for (auto &block : file.second.blocks) {
                    if (block.data == blkData) {
                        // Add new node
                        block.dataNodeIDs.push_back(newNodeId);
                        // Remove donor node if it exists
                        auto it = find(block.dataNodeIDs.begin(), block.dataNodeIDs.end(), donorNode);
                        if (it != block.dataNodeIDs.end()) {
                            block.dataNodeIDs.erase(it);
                        }
                        break;
                    }
                }
            }
            printInfo("Moved block from Node " + to_string(donorNode) + " to Node " + to_string(newNodeId));
        }
    }

    // Also replicate missing blocks if any file has fewer than REPLICATION_FACTOR replicas
    for (auto &file : nameNode) {
        for (auto &block : file.second.blocks) {
            if (block.dataNodeIDs.size() < REPLICATION_FACTOR) {
                dataNodes[newNodeId].push_back(block.data);
                block.dataNodeIDs.push_back(newNodeId);
                printInfo("Replicated block to new Node " + to_string(newNodeId));
            }
        }
    }

    printSuccess("Load balancing complete for Node " + to_string(newNodeId));
    saveAllNodeData();
    saveMetadata();
}

void addDataNode() {
    lock_guard<mutex> lock(mtx);
    dataNodes.push_back({});
    nodeAlive.push_back(true);
    
    // Create directory for new node
    fs::create_directories(STORAGE_DIR + "/node_" + to_string(NUM_DATANODES));
    
    NUM_DATANODES++;
    printSuccess("Added new DataNode. Total nodes: " + to_string(NUM_DATANODES));
    balanceNewNode(NUM_DATANODES - 1);
}

void removeDataNode(int id) {
    lock_guard<mutex> lock(mtx);
    if (!isValidNode(id)) {
        printError("Invalid node ID!");
        return;
    }
    
    // Update metadata to remove references to this node
    for (auto &file : nameNode) {
        for (auto &block : file.second.blocks) {
            block.dataNodeIDs.erase(
                remove(block.dataNodeIDs.begin(), block.dataNodeIDs.end(), id),
                block.dataNodeIDs.end());
        }
    }
    
    dataNodes.erase(dataNodes.begin() + id);
    nodeAlive.erase(nodeAlive.begin() + id);
    NUM_DATANODES--;
    
    printWarning("Removed DataNode " + to_string(id) + ". Total nodes: " + to_string(NUM_DATANODES));
    saveAllNodeData();
    saveMetadata();
}

void deleteFile(const string &filename) {
    lock_guard<mutex> lock(mtx);

    if (!nameNode.count(filename)) {
        printError("File not found!");
        return;
    }

    auto &meta = nameNode[filename];
    for (auto &block : meta.blocks) {
        for (int id : block.dataNodeIDs) {
            if (isValidNode(id)) {
                auto &node = dataNodes[id];
                auto it = find(node.begin(), node.end(), block.data);
                if (it != node.end())
                    node.erase(it);
            }
        }
    }

    nameNode.erase(filename);
    printSuccess("File '" + filename + "' deleted successfully.");
    saveAllNodeData();
    saveMetadata();
}

void printMenu() {
    printHeader("MINI HDFS SIMULATOR");
    cout << BOLD << CYAN << "| " << setw(25) << left << "1. Upload File" 
         << " | " << setw(25) << "2. List Files" << " |" << RESET << endl;
    cout << CYAN << "| " << setw(25) << "3. Show Metadata" 
         << " | " << setw(25) << "4. Read File" << " |" << RESET << endl;
    cout << CYAN << "| " << setw(25) << "5. Kill Node" 
         << " | " << setw(25) << "6. Recover Node" << " |" << RESET << endl;
    cout << CYAN << "| " << setw(25) << "7. Add DataNode" 
         << " | " << setw(25) << "8. Remove DataNode" << " |" << RESET << endl;
    cout << CYAN << "| " << setw(25) << "9. Delete File" 
         << " | " << setw(25) << "10. Exit" << " |" << RESET << endl;
    cout << CYAN << "+-----------------------------------------------------------+" << RESET << endl;
}

// ---------- MAIN ----------
int main() {
    srand(time(0));
    dataNodes.resize(NUM_DATANODES);
    nodeAlive.resize(NUM_DATANODES, true);

    // Initialize storage system
    createDirectories();
    loadStorage();

    thread hbThread(heartbeat);

    cout << BOLD << MAGENTA << "\nMini HDFS Simulator with Persistent Storage Initialized!" << RESET << endl;
    cout << "Storage Directory: " << STORAGE_DIR << endl;
    cout << "Metadata Directory: " << METADATA_DIR << endl;

    while (true) {
        printMenu();
        cout << BOLD << "Select option [1-10]: " << RESET;
        
        int choice;
        if (!(cin >> choice)) {
            cin.clear();
            cin.ignore(numeric_limits<streamsize>::max(), '\n');
            printError("Invalid input! Please enter a number.");
            continue;
        }
        
        if (choice == 10) break;

        string filename;
        int id;
        switch (choice) {
        case 1: {
            cout << "Enter filename to upload: ";
            cin >> filename;
            ifstream in(filename);
            if (!in.is_open()) {
                printError("Could not open file '" + filename + "'!");
                break;
            }
            string line, content;
            while (getline(in, line))
                content += line + "\n";
            uploadFile(filename, content);
            break;
        }
        case 2:
            listFiles();
            break;
        case 3: {
            cout << "Enter filename: ";
            cin >> filename;
            showMetadata(filename);
            break;
        }
        case 4: {
            cout << "Enter filename: ";
            cin >> filename;
            readFile(filename);
            break;
        }
        case 5: {
            cout << "Enter node ID to kill: ";
            if (!(cin >> id)) {
                cin.clear();
                cin.ignore(numeric_limits<streamsize>::max(), '\n');
                printError("Invalid input! Please enter a number.");
                break;
            }
            killNode(id);
            break;
        }
        case 6: {
            cout << "Enter node ID to recover: ";
            if (!(cin >> id)) {
                cin.clear();
                cin.ignore(numeric_limits<streamsize>::max(), '\n');
                printError("Invalid input! Please enter a number.");
                break;
            }
            recoverNode(id);
            break;
        }
        case 7:
            addDataNode();
            break;
        case 8: {
            cout << "Enter node ID to remove: ";
            if (!(cin >> id)) {
                cin.clear();
                cin.ignore(numeric_limits<streamsize>::max(), '\n');
                printError("Invalid input! Please enter a number.");
                break;
            }
            removeDataNode(id);
            break;
        }
        case 9: {
            cout << "Enter filename to delete: ";
            cin >> filename;
            deleteFile(filename);
            break;
        }
        default:
            printError("Invalid option!");
        }
    }

    heartbeatRunning = false;
    if (hbThread.joinable()) {
        hbThread.join();
    }

    printHeader("SIMULATION ENDED");
    cout << GREEN << "Thank you for using Mini HDFS Simulator!" << RESET << endl;
    return 0;
}