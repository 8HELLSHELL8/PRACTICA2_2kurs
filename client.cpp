#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <string>
using namespace std;

int createClientSocket()
{
    int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd == -1)
    {
        cout << "client`s socket creation is failed!";
        exit(-1);
    }
    return socket_fd;
}

sockaddr_in defineServer()
{
    sockaddr_in serverAddress;
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(7432);
    serverAddress.sin_addr.s_addr = INADDR_ANY;
    return serverAddress;
}

void sendToServer(string clientMessage, int clientSocket)
{
    const char* message = clientMessage.c_str();
    send(clientSocket, message, strlen(message), 0);
}

void clientMenu()
{

    int clientSocket = createClientSocket(); //CREATING CLIENT`S SOCKET 

    sockaddr_in serverAddress = defineServer(); //DEFINING SERVER`S ADDRESS 
     
    if (connect(clientSocket,(struct sockaddr*)&serverAddress, sizeof(serverAddress)) == -1) //CONNECTING TO THE SERVER
    {
        cout << "Connection to the server wasn`t established!" << endl;
        exit(-1);
    }
    else cout << "Connection established properly!" << endl;

    while (true)
    {
        string input;
        cout << "Welcome to database!" << endl;
        cout <<"<------------------------------->" << endl;
        cout << "Choose operation: " << endl;
        cout << "Input query to database: 1" << endl;
        cout << "Exit client: 0" << endl;
        
        switch (stoi(input))
        {
        case 0:
        {
            cout << "Exiting client!" << endl;
            close(clientSocket);
            exit(-1);
        }
        case 1:
        {
            string userInput;
            cout << "Enter your query: ";
            getline(cin, userInput);
            sendToServer(userInput, clientSocket);
            break;
        }
        default:
            cout << "Wrong operation chosen!" << endl;
            break;
        }
    }    
}


int main()
{

    clientMenu();

    return 0;
}