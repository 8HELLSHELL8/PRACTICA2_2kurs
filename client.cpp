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
        cout << "Ошибка создания сокета клиента!" << endl;
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

void sendToServer(const string& clientMessage, int clientSocket)
{
    const char* message = clientMessage.c_str();
    send(clientSocket, message, strlen(message), 0);
}

void clientMenu()
{
    int clientSocket = createClientSocket(); // Создаем клиентский сокет

    sockaddr_in serverAddress = defineServer(); // Определяем адрес сервера
     
    if (connect(clientSocket, (struct sockaddr*)&serverAddress, sizeof(serverAddress)) == -1) // Подключаемся к серверу
    {
        cout << "Подключение к серверу не установлено!" << endl;
        close(clientSocket);
        exit(-1);
    }
    else 
    {
        cout << "Подключение установлено успешно!" << endl;
    }

    // Основной цикл работы клиента
    while (true)
    {
        cout << "\n<------------------------------->" << endl;
        cout << "Добро пожаловать в базу данных!" << endl;
        cout << "<------------------------------->" << endl;
        cout << "Выберите операцию:" << endl;
        cout << "1. Ввести запрос к базе данных (команда: use)" << endl;
        cout << "2. Выйти из клиента (команда: exit)" << endl;
        cout << "<------------------------------->" << endl;

        string input;
        cout << "Введите команду: ";
        getline(cin, input);

        if (input == "exit" || input == "EXIT") 
        {
            cout << "Завершение работы клиента!" << endl;
            break;
        }
        else if (input == "use" || input == "USE")
        {
            string userInput;
            cout << "Input your query: ";
            getline(cin, userInput);
            sendToServer(userInput, clientSocket);
            cout << "Query sent to server" << endl;
        }
        else 
        {
            cout << "Error: wrong operation!" << endl;
        }
    }

    close(clientSocket);
    cout << "Client disconnected" << endl;
}

int main()
{
    clientMenu();
    return 0;
}
