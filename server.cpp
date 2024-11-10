//FOR Programm
#include <iostream>
#include "headers/LinkedList.h"
#include "headers/HashTable.h"
#include <cmath>
#include <string>

//FOR SERVER
#include <sys/socket.h>
#include <unistd.h>
#include <netinet/in.h>
#include <cstring>

//FOR multitreading
#include <thread>
#include <mutex>

//FOR using files in system
#include <filesystem>

//FOR Parsing
#include <cjson/cJSON.h>
#include <fstream>
#include <sstream>

//FOR DataBase


using namespace std;

mutex mtx;
mutex delMutex;
mutex insertMutex;
mutex selectMutex;



string getLastFolderName(const string& path) {

    size_t lastSlashPos = path.rfind('/');

    if (lastSlashPos != std::string::npos) {
        return path.substr(lastSlashPos + 1);
    }
    
    return path;
}

void unlockTable(const string& pathToDir)
{
    
    string tableName = getLastFolderName(pathToDir);
    ofstream lockFile(pathToDir + "/" + tableName + "_lock");
    lockFile << 0;
    lockFile.close();
}

void lockTable(const string& pathToDir)
{
    
    string tableName = getLastFolderName(pathToDir);
    ofstream lockFile(pathToDir + "/" + tableName + "_lock");
    lockFile << 1;
    lockFile.close();
}

void increasePKSEQ(const string& tableName)
{
    string pathToDir = filesystem::current_path(); //Getting table name
    pathToDir += "/" + tableName;

    string fileInput;
    ifstream PKSEQread(pathToDir + "/" + tableName + "_pk_sequence"); //Opening line counter
    if (!PKSEQread.is_open())
    {
        throw runtime_error("Error opening pk_sequence and reading it");
    }
    getline(PKSEQread, fileInput);
    PKSEQread.close();

    int increasedLinesAmount = stoi(fileInput) + 1;

    ofstream PKSEQupload(pathToDir + "/" + tableName + "_pk_sequence");
    PKSEQupload << increasedLinesAmount;
    PKSEQupload.close();
    
}

void decreasePKSEQ(const string& tableName)
{
    string pathToDir = filesystem::current_path(); //Getting table name
    pathToDir += "/" + tableName;

    string fileInput;
    ifstream PKSEQread(pathToDir + "/" + tableName + "_pk_sequence"); //Opening line counter
    if (!PKSEQread.is_open())
    {
        throw runtime_error("Error opening pk_sequence and reading it");
    }
    getline(PKSEQread, fileInput);
    PKSEQread.close();

    int decreasedLinesAmount = stoi(fileInput) - 1;

    ofstream PKSEQupload(pathToDir + "/" + tableName + "_pk_sequence");
    PKSEQupload << decreasedLinesAmount;
    PKSEQupload.close();
}

string readJSON(const string& fileName) //Reading json content in string line
{ 
    fstream file(fileName);
    if (!file.is_open())
    {
        throw runtime_error("Error opening " + fileName + ".json file!");
    } 

    stringstream buffer;
    buffer << file.rdbuf();
    file.close();

    return buffer.str();
}

bool createDir(const string& dirName)
{
    string pathToDir = filesystem::current_path();
    pathToDir += "/" + dirName;
    if (filesystem::create_directory(pathToDir)) return true;
    else return false;
}

void createFilesInSubFolder(const cJSON* table, const cJSON* structure, const string& subName)
{
    

    LinkedList<string> columnNames; 
    cJSON* tableArray = cJSON_GetObjectItem(structure, table->string); //Reading table column names
    int arrSize = cJSON_GetArraySize(tableArray); //Reading amount of columns in table

    for (size_t i = 0; i < arrSize; i++) //Insert column names in table
    {
        cJSON* arrayItem = cJSON_GetArrayItem(tableArray, i);
        columnNames.addtail(arrayItem->valuestring);
    }
    string pathToDir = filesystem::current_path();
    pathToDir += subName;
    
    ofstream CSV(pathToDir + "/1.csv"); //Create and fill up .csv table
    for (size_t i = 0; i < columnNames.size(); i++)
    {   
        if (i < columnNames.size()-1)
        {
            CSV << columnNames.get(i) << ",";
        }
        else
        {
            CSV << columnNames.get(i);
        }
        
    }
    CSV << endl;
    CSV.close();

    string pathToDirPQ = pathToDir + "/" + table->string + "_pk_sequence";
    ofstream PKSEQ(pathToDirPQ); //Creating file-counter for each table
    PKSEQ << "1";
    PKSEQ.close();

    unlockTable(pathToDir);
}

void createDataBase()
{   
 
    LinkedList<string> tablePaths;
    string jsonContent = readJSON("schema.json"); //Reading json
    cJSON* json = cJSON_Parse(jsonContent.c_str()); //Parsing .json file
    cJSON* schemaName = cJSON_GetObjectItem(json, "name"); //Parsing DataBase name
    string DataBaseName = schemaName->valuestring;

    if (jsonContent.empty())
    {
        throw runtime_error("Error reading schema, content is empty!");
    }

    fstream checkDB("DataBaseFlag");
    if (checkDB.is_open())
    {
        string path = filesystem::current_path();
        filesystem::current_path(path + "/" + DataBaseName);
        checkDB.close();
        return;
    }

    ofstream DataBaseFlag("DataBaseFlag"); //Creating presence database flag
    DataBaseFlag.close();

    

    
    if (json == nullptr)
    {
        throw runtime_error("Error parsing schema file!");
    }

    cJSON* schemaLimit = cJSON_GetObjectItem(json, "tuples_limit"); //Parsing tuple limit
    int tuplesLimit = schemaLimit->valueint;

    
    
    createDir(DataBaseName); //Creating DataBase folder

    string path = filesystem::current_path();

    filesystem::current_path(path + "/" + DataBaseName);

    cJSON* structure = cJSON_GetObjectItem(json,"structure"); //Parsing structure

    for (cJSON* table = structure->child; table != nullptr; table = table->next) //Going through tables
    {
        string subFolderPath = filesystem::current_path();
        string tableName = table->string;
        subFolderPath =  "/" + tableName;
        tablePaths.addhead(subFolderPath);

        createDir(subFolderPath);

        createFilesInSubFolder(table, structure, subFolderPath);
    }
    cJSON_Delete(json);
}

int getPKSEQ(string tableName)
{
    string pathToDir = filesystem::current_path(); //Getting table name
    pathToDir += "/" + tableName;

    string fileInput;
    
    ifstream PKSEQ(pathToDir + "/" + tableName + "_pk_sequence"); //Opening line counter
    if (!PKSEQ.is_open())
    {
        cout << pathToDir + "/" + tableName + "_pk_sequence";
        throw runtime_error("Error opening pk_sequence and reading it");
    }
    getline(PKSEQ, fileInput);
    PKSEQ.close();

    return stoi(fileInput);
}

LinkedList<string> getColumnNamesFromTable(string tableName)
{
    string pathToDir = filesystem::current_path();
    pathToDir += "/" + tableName;

    ifstream COLNAMES(pathToDir + "/1.csv");
    string fileInput;
    getline(COLNAMES, fileInput, '\n');
    
    LinkedList<string> columnNames;
    string word = "";
    for (auto symbol : fileInput) //GETTING column names
    {
        if (symbol == ',')
        {
            columnNames.addtail(word);
            word = "";
            continue;
        }
        word += symbol;
    }
    if (!word.empty()) columnNames.addtail(word);
    COLNAMES.close();

    return columnNames;
}

LinkedList<HASHtable<string>> getTableLines(const string& tableName)
{
    
    LinkedList<HASHtable<string>> thisTable;
    int amountOfLinesInTable = getPKSEQ(tableName);
    LinkedList<string> columnNames = getColumnNamesFromTable(tableName);
    
    int filesCounter = ceil(static_cast<double>(amountOfLinesInTable)/1000); //Counting amount of .csv files
    for (int i = 0; i < filesCounter; i++)
    {
        int startRow = i * 1000;
        int endRow = min(startRow + 1000, amountOfLinesInTable);
        string fileInput;

        string pathToDir = filesystem::current_path();
        pathToDir += "/" + tableName;

        ifstream CSV(pathToDir + "/" + to_string(i+1) + ".csv");
        for (int row = startRow; row < endRow; row++)
        {
            getline(CSV, fileInput, '\n');
            HASHtable<string> tableLine(columnNames.size());
            string word = "";
            int wordCounter = 0;
            for (auto symbol : fileInput) //Process line
            {
                if (symbol == ',')
                {
                    tableLine.HSET(columnNames.get(wordCounter), word);
                    word = "";
                    wordCounter++;
                    continue;
                }
                word += symbol;
            }
            if (!word.empty()) tableLine.HSET(columnNames.get(wordCounter), word);
            thisTable.addtail(tableLine);
        }
        CSV.close();
    }
    
    return thisTable;
}

LinkedList<HASHtable<string>> readTable(const string& tableName)
{
    LinkedList<HASHtable<string>> thisTable;
    string pathToDir = filesystem::current_path();
    pathToDir += "/" + tableName;

    int amountLines = getPKSEQ(tableName);
    int fileCount = ceil(static_cast<double>(amountLines) / 1000);
    
    for (int i = 0; i < fileCount; ++i) //Creating CSV if >1000 elements
    {
        fstream fileCSV(pathToDir + "/" + to_string(i+1) + ".csv");
        if (!fileCSV.good())
        {
            ofstream newFile(pathToDir + "/" + to_string(i+1) + ".csv");
            newFile.flush();
            newFile.close();
        }
        fileCSV.flush();
        fileCSV.close();
    }

    thisTable = getTableLines(tableName);
    return thisTable;
}

void uploadTable(LinkedList<HASHtable<string>> table, string tableName)
{
    int linesAmount = getPKSEQ(tableName);
    int fileCount = ceil(static_cast<double>(linesAmount) / 1000);
    LinkedList<string> columnNames = getColumnNamesFromTable(tableName);
    string pathToDir = filesystem::current_path();
    pathToDir += "/" + tableName;
    for (int i = 0; i < fileCount; ++i) 
    {
        int startRow = i * 1000;
        int endRow = min(startRow + 1000, linesAmount);
        ofstream UPLOAD(pathToDir + "/" + to_string(i + 1) + ".csv", ios::out | ios::trunc);
        if (!UPLOAD.is_open()) throw runtime_error("Error opening csv for table upload");
        for (int row = startRow; row < endRow; row++)
        {
            for (int column = 0; column < columnNames.size(); column++)
            {
                auto currentRow = table.get(row);

                if (column == columnNames.size() - 1)
                {
                    UPLOAD << currentRow.HGET(columnNames.get(column));
                    UPLOAD << "\n";
                }
                else
                {
                    UPLOAD << currentRow.HGET(columnNames.get(column));
                    UPLOAD << ",";
                }
                
            }
        }
        UPLOAD.flush();
        UPLOAD.close();
    }
    
}

void insert(LinkedList<string> values, string tableName)
{
    lock_guard<mutex> insertionMUTEX(insertMutex);
    lockTable(tableName);
    LinkedList<HASHtable<string>> table = readTable(tableName);
    
    LinkedList<string> columnNames = getColumnNamesFromTable(tableName);
    HASHtable<string> row(columnNames.size());
    if (values.size() == columnNames.size())
    {   
        for (int i = 0; i < columnNames.size(); i++)
        {
            row.HSET(columnNames.get(i),values.get(i));
        }
        table.addtail(row);
        increasePKSEQ(tableName);
    }
    else if (values.size() < columnNames.size())
    {
        for (int i = 0; i < columnNames.size(); i++)
        {
            if (i >= values.size())
            {
                row.HSET(columnNames.get(i),"EMPTY");
            }
            else
            {
                row.HSET(columnNames.get(i),values.get(i));
            }
        }
        table.addtail(row);
        increasePKSEQ(tableName);
    }
    else
    {
        unlockTable(tableName);
        throw runtime_error("Amount of values more than columns in table!");
    }

    uploadTable(table, tableName);

    unlockTable(tableName);
}

LinkedList<string> parseCommand(string userInput)
{
    LinkedList<string> dividedInput;
    string word = "";
    for (auto symbol : userInput)
    {
        if (symbol == '\'' || symbol == '(' || symbol == ')' || symbol == ' ' || symbol == ',')
        {
            if (!word.empty())
            {
                dividedInput.addtail(word);
            }
            word = "";
            continue;
        }
        word += symbol;
    }
    return dividedInput;
}

string parseTablenameForInsert(LinkedList<string> commandList)
{
    return commandList.get(2);
}

LinkedList<string> parseValuesForInsert(LinkedList<string> commandList)
{   
    LinkedList<string> values;
    if (commandList.get(3) != "VALUES")
    {
        throw runtime_error("Syntax error in pasing values for INSERT");
    }
    for (int i = 4; i < commandList.size(); i++)
    {
        values.addtail(commandList.get(i));
    }
    return values;
}

bool whereInside(LinkedList<string> commandList)
{
    return commandList.search("WHERE");
}

void handleINPUT(LinkedList<string> commandList)
{
    if (commandList.get(0) == "INSERT" && commandList.get(1) == "INTO" && whereInside(commandList) == 0)
    {
        LinkedList<string> values = parseValuesForInsert(commandList);
        string tableName = parseTablenameForInsert(commandList);
        insert(values, tableName);
    }
    else
    {
        throw runtime_error("Syntax error in input query!");
    }
}

bool isTableName(const string& element)
{
    for (auto sym : element)
    {
        if(sym == '.') return true;
    }
    return false;
}

string divideAndGetTable(const string& word)
{
    string tableName = "";
    for (auto sym : word)
    {
        if (sym == '.') return tableName;

        else tableName += sym;
    }
    return "";
}   

string divideAndGetColumn(const string& word)
{
    bool writeMode = 0;
    string tableName = "";
    for (auto sym : word)
    {
        if (writeMode == 1)
        {
            tableName += sym;
        }
        if (sym == '.') writeMode = 1;
    }
    return tableName;
}

LinkedList<string> getSelectedTablesFROM(LinkedList<string> commandList)
{
    LinkedList<string> selected;
    
    bool writeMode = 0;
    for (int i = 0; i < commandList.size(); i++)
    {
        auto token = commandList.get(i);
        if (token == "WHERE") break;
        if ( writeMode == 1) selected.addtail(token);
        if (token == "FROM") writeMode = 1;

    }
    
    return selected;
}

LinkedList<string> getSelectedTablesSELECT(LinkedList<string> commandList)
{
    LinkedList<string> selected;
    
    bool writeMode = 0;
    for (int i = 0; i < commandList.size(); i++)
    {
        auto token = commandList.get(i);
        if (token == "FROM") break;
        if ( writeMode == 1) selected.addtail(token);
        if (token == "SELECT") writeMode = 1;

    }
    
    return selected;
}


bool getFinalResult(LinkedList<bool> results, LinkedList<string> operators)
{
    bool finalRes;
    if (operators.size() == 0) return results.get(0);
    else
    {
         for (int i = 0; i < results.size() - 1; i++)
        {
            if (i == 0) finalRes = results.get(0);
            if (operators.get(i)== "AND")
            {
                finalRes = finalRes && results.get(i + 1);
            }
            else if (operators.get(i)== "OR")
            {
                finalRes = finalRes || results.get(i + 1);
            }
        }
    }
   
    return finalRes;
}

bool checkCondition(string table1Name, HASHtable<string> row1, 
                    string table2Name, HASHtable<string> row2, 
                    LinkedList<string> conditions, LinkedList<string> operators)
{

    LinkedList<bool> results;


    for (int i = 0; i < conditions.size(); i += 3)
    {
        string left = conditions.get(i);
        string op = conditions.get(i + 1);
        string right = conditions.get(i + 2);

        if (isTableName(left) && op == "=")
        {
            if (divideAndGetTable(left) ==  table1Name)
            {
                left = row1.HGET(divideAndGetColumn(left));
            }
            else if (divideAndGetTable(left) ==  table2Name)
            {
                left = row2.HGET(divideAndGetColumn(left));
            }
            else
            {
                throw runtime_error("Wrong table name used");
            }


            if (isTableName(right))
            {
                if (divideAndGetTable(right) ==  table1Name)
                {
                    right = row1.HGET(divideAndGetColumn(left));
                }
                else if (divideAndGetTable(right) ==  table2Name)
                {
                    right = row2.HGET(divideAndGetColumn(right));
                }
                else
                {
                    throw runtime_error("Wrong table name used");
                }
            }

            results.addtail(left == right); 
        }
        else
        {
            throw runtime_error("Wrong syntax in chosen columns");
        }
    }

    return getFinalResult(results, operators);

}

void handleSELECT(LinkedList<string> inputList, int clientSocket)
 {
    lock_guard<mutex> selectionMUTEX(selectMutex);
    LinkedList<string> selectedColumns = getSelectedTablesSELECT(inputList);
    LinkedList<string> selectedTables = getSelectedTablesFROM(inputList);

    LinkedList<string> conditions;
    LinkedList<string> operators;
    
    bool startWrite = 0;
    string element;
    for (int i = 0; i < inputList.size(); i++)
    {
        element = inputList.get(i);
        if (startWrite)
        {
            if (element == "OR" || element == "AND")
            {
                operators.addtail(element);
            }
            else
            {
                conditions.addtail(element);
            }
        }
        if (element == "WHERE") startWrite = 1;
        
    }

    if (selectedTables.size() == 2 && selectedColumns.size() == 2)
    {

        LinkedList<HASHtable<string>> table1 = readTable(selectedTables.get(0));
        LinkedList<HASHtable<string>> table2 = readTable(selectedTables.get(1));

        LinkedList<string> table1ColNames = getColumnNamesFromTable(selectedTables.get(0));
        LinkedList<string> table2ColNames = getColumnNamesFromTable(selectedTables.get(1));

        for (int i = 1; i < table1.size(); i++)
        {
            HASHtable<string> currentRowFirst = table1.get(i);
            for (int j = 1; j < table2.size(); j++)
            {
                HASHtable<string> currentRowSecond = table2.get(j);
                if (checkCondition(selectedTables.get(0),currentRowFirst, selectedTables.get(1), currentRowSecond, conditions, operators))
                {
                    string outputForClient = table1.get(i).HGET(divideAndGetColumn(selectedColumns.get(0))) + " " 
                    + table2.get(j).HGET(divideAndGetColumn(selectedColumns.get(1))) + '\n';
                    send(clientSocket, outputForClient.c_str(), outputForClient.size(), 0);
                }

            }

        }
    }
    else
    {
        throw runtime_error("Wrong amount of tables chosen!");
    }
}

bool checkCondition(string table1Name, HASHtable<string> row1, 
                    LinkedList<string> conditions, LinkedList<string> operators)
{

    LinkedList<bool> results;


    for (int i = 0; i < conditions.size(); i += 3)
    {
        string left = conditions.get(i);
        string op = conditions.get(i + 1);
        string right = conditions.get(i + 2);

        if (isTableName(left) && op == "=" && !isTableName(right))
        {
            if (divideAndGetTable(left) ==  table1Name)
            {
                left = row1.HGET(divideAndGetColumn(left));
            }
            else
            {
                throw runtime_error("Wrong condition for delete");
            }

            results.addtail(left == right); 
        }
        else
        {
            throw runtime_error("Wrong syntax in delete");
        }
    }

    return getFinalResult(results, operators);

}

void handleDELETE(LinkedList<string> inputList)
{
    lock_guard<mutex> deletionMUTEX(delMutex);
    LinkedList<string> selectedTables = getSelectedTablesFROM(inputList);
    string tableName = selectedTables.get(0);
    LinkedList<HASHtable<string>> table = readTable(tableName);

    lockTable(tableName);

    LinkedList<string> conditions;
    LinkedList<string> operators;
    
    bool startWrite = 0;
    string element;
    for (int i = 0; i < inputList.size(); i++)
    {
        element = inputList.get(i);
        if (startWrite)
        {
            if (element == "OR" || element == "AND")
            {
                operators.addtail(element);
            }
            else
            {
                conditions.addtail(element);
            }
        }
        if (element == "WHERE") startWrite = 1;
        
    }

    LinkedList<HASHtable<string>> newTable;

    if (selectedTables.size() == 1)
    {
        for(int i = 0; i < table.size(); i++)
        {
            HASHtable<string> currentRow = table.get(i);
            if (!checkCondition(tableName, currentRow, conditions, operators))
            {
                newTable.addtail(currentRow);
            }
            else decreasePKSEQ(tableName);
        }

        uploadTable(newTable, tableName);
    } 
    else
    {
        throw runtime_error("Wrong syntax in delete from table");
    }

    unlockTable(tableName);
}

void MENU(auto clientInput, auto clientSocket)
{
    
    LinkedList<string> inputList = parseCommand(clientInput);
    inputList.print();
    string operation = inputList.get(0);
    
    if (operation == "exit" || operation == "EXIT")
    {
        cout << "Waiting for next query" << endl;
        return;
    }

    if (operation == "SELECT")
    {
        handleSELECT(inputList, clientSocket);
    }
    else if (operation == "DELETE")
    {
        handleDELETE(inputList);
    }
    else if (operation == "INSERT")
    {
        handleINPUT(inputList);
    }
    else
    {
        throw runtime_error("Wrong operation called!");
    }    
}



sockaddr_in defineServer()
{
    sockaddr_in serverAddress;
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(7432);
    serverAddress.sin_addr.s_addr = INADDR_ANY;
    return serverAddress;
}

void handleClient(int clientSocket)
{
    char buffer[1024];
    
    int bytesReceived = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
    if (bytesReceived > 0) {
        buffer[bytesReceived] = '\0';
        {
            lock_guard<mutex> lock(mtx);
            cout << "Received message: " << buffer << endl;
        }

        MENU(buffer, clientSocket);
    }

    {
        lock_guard<mutex> lock(mtx);
        cout << "Client disconnected." << endl;
    }
    close(clientSocket); // Закрываем клиентский сокет после обработки одного запроса
}

void serverListener(int serverSocket)
{
    sockaddr_in clientAddress;
    socklen_t clientAddressSize = sizeof(clientAddress);

    while (true)
    {
        int clientSocket = accept(serverSocket, (sockaddr*)&clientAddress, &clientAddressSize);
        if (clientSocket == -1)
        {
            cerr << "Error occurred while connecting to client!" << endl;
            continue;
        }

        {
            lock_guard<mutex> lg(mtx);
            cout << "Client connected!" << endl;
        }

        // Создаем поток для каждого клиента, чтобы обеспечить многопоточность
        thread clientThread(handleClient, clientSocket);
        clientThread.detach(); // Отсоединяем поток для независимой обработки клиента
    }
}

void serverHandling()
{
    int serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket == -1)
    {
        cout << "Server socket creation failed!";
        exit(-1);
    }

    sockaddr_in serverAddress = defineServer();
    if (bind(serverSocket, (sockaddr*)&serverAddress, sizeof(serverAddress)) == -1)
    {
        cout << "Error binding socket";
        exit(-1);
    }

    listen(serverSocket, 5);
    cout << "Server started and listening on port 7432" << endl;

    serverListener(serverSocket); // Запускаем обработчик входящих подключений

    close(serverSocket);
}


int main()
{
    setlocale(LC_ALL, "RU");
    createDataBase();

    serverHandling();





    return 0;
}
