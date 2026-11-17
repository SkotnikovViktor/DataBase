#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#define PORT 6767
#define BUFFER_SIZE 1024

int main() {
    int server_fd, client_fd;
    char buffer[BUFFER_SIZE];
    char client_ip[INET_ADDRSTRLEN];
    struct sockaddr_in server_addr, client_addr;
    socklen_t addr_len;

    // 1. Создаем TCP-сокет (SOCK_STREAM)
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("Ошибка создания сокета");
        exit(EXIT_FAILURE);
    }

    // Позволяет перезапускать сервер сразу, не дожидаясь освобождения порта ОС
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // 2. Настраиваем адрес и порт
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT);

    // 3. Привязываем сокет к порту
    if (bind(server_fd, (const struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("Ошибка bind");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    // 4. Начинаем слушать входящие подключения
    if (listen(server_fd, 5) < 0) {
        perror("Ошибка listen");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    printf("TCP-сервер запущен и ждет подключений на порту %d...\n", PORT);

    while (1) {
        addr_len = sizeof(client_addr);

        // 5. Принимаем клиента (создается отдельный сокет client_fd)
        client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &addr_len);
        if (client_fd < 0) {
            perror("Ошибка accept");
            continue;
        }

        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
        int client_port = ntohs(client_addr.sin_port);
        printf("[+] Подключился клиент %s:%d\n", client_ip, client_port);

        // 6. Читаем данные от этого клиента, пока он не закроет соединение
        while (1) {
            ssize_t bytes_received = recv(client_fd, buffer, BUFFER_SIZE - 1, 0);

            if (bytes_received < 0) {
                perror("Ошибка recv");
                break;
            }
            if (bytes_received == 0) {
                // recv вернул 0 — клиент штатно разорвал соединение
                printf("[-] Клиент %s:%d отключился\n", client_ip, client_port);
                break;
            }

            buffer[bytes_received] = '\0';
            printf("[%s:%d] Прислал (%zd байт): %s", client_ip, client_port, bytes_received, buffer);
        }

        // 7. Обязательно закрываем сокет конкретного клиента
        close(client_fd);
    }

    close(server_fd);
    return 0;
}

