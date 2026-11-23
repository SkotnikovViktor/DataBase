#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define DEFAULT_PORT 6767
#define BUFFER_SIZE 2048
#define TERMINATOR ".\n"   /* строка-терминатор конца ответа сервера */

/* Надёжная отправка всех данных */
static int send_all(int sock, const char *buf, size_t len) {
    size_t total = 0;
    while (total < len) {
        ssize_t n = send(sock, buf + total, len - total, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1; /* соединение разорвано */
        total += (size_t)n;
    }
    return 0;
}

/*
 * Чтение ответа сервера до строки-терминатора ".\n".
 * Сервер (в исправленной версии) после каждого ответа шлёт ".\n".
 * Если сервер ещё не исправлен — клиент всё равно не зависнет:
 * он завершит чтение, как только получит строку, начинающуюся с "OK" или "ERR"
 * и заканчивающуюся '\n', и не начинающуюся с "OK LIST BEGIN".
 *
 * Возврат: 0 — ответ получен, -1 — ошибка/разрыв соединения.
 */
static int receive_response(int sock) {
    char buffer[BUFFER_SIZE];
    char line[BUFFER_SIZE];
    size_t line_len = 0;

    while (1) {
        ssize_t n = recv(sock, buffer, sizeof(buffer) - 1, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            perror("Ошибка при чтении ответа");
            return -1;
        }
        if (n == 0) {
            printf("\n[Сервер разорвал соединение]\n");
            return -1;
        }
        buffer[n] = '\0';

        /* Разбираем полученные данные построчно */
        for (ssize_t i = 0; i < n; i++) {
            char c = buffer[i];

            if (c == '\n') {
                /* Убираем возможный '\r' в конце строки */
                if (line_len > 0 && line[line_len - 1] == '\r') {
                    line_len--;
                }
                line[line_len] = '\0';

                /* Проверяем терминатор */
                if (line_len == 1 && line[0] == '.') {
                    fflush(stdout);
                    return 0;
                }

                /* Выводим строку */
                printf("%s\n", line);
                fflush(stdout);

                /*
                 * Резервная эвристика на случай, если сервер ещё не шлёт ".":
                 * если это одиночный ответ OK/ERR (не BEGIN списка) — считаем ответ законченным.
                 */
                if (line_len >= 3 && strncmp(line, "OK ", 3) == 0 &&
                    strncmp(line, "OK LIST BEGIN", 13) != 0) {
                    return 0;
                }
                if (line_len >= 4 && strncmp(line, "ERR ", 4) == 0) {
                    return 0;
                }
                if (line_len >= 12 && strncmp(line, "OK LIST END", 11) == 0) {
                    return 0;
                }

                line_len = 0;
            } else {
                if (line_len + 1 < sizeof(line)) {
                    line[line_len++] = c;
                } else {
                    /* Строка слишком длинная — сбрасываем, чтобы не зациклиться */
                    line_len = 0;
                }
            }
        }
    }
}

int main(int argc, char *argv[]) {
    int sockfd;
    struct sockaddr_in serv_addr;
    char input_cmd[BUFFER_SIZE];
    const char *host = "127.0.0.1";
    int port = DEFAULT_PORT;

    if (argc > 1) host = argv[1];
    if (argc > 2) port = atoi(argv[2]);

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("Ошибка создания сокета");
        exit(EXIT_FAILURE);
    }

    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host, &serv_addr.sin_addr) <= 0) {
        perror("Неверный адрес сервера");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("Ошибка подключения к серверу");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    printf("Подключено к серверу %s:%d\n", host, port);
    printf("Введите команды (например: CREATE DB test, OPEN test,\n");
    printf("  INSERTBACK INTO test VALUES (hello;world), SELECT *, SELECT 1).\n");
    printf("Для выхода введите 'exit' или 'quit'.\n");

    while (1) {
        printf("> ");
        fflush(stdout);

        if (fgets(input_cmd, sizeof(input_cmd), stdin) == NULL) {
            break; /* EOF (Ctrl+D) */
        }

        size_t len = strlen(input_cmd);
        if (len > 0 && input_cmd[len - 1] == '\n') {
            input_cmd[len - 1] = '\0';
        }

        if (strcmp(input_cmd, "exit") == 0 || strcmp(input_cmd, "quit") == 0) {
            break;
        }

        if (strlen(input_cmd) == 0) {
            continue;
        }

        /* Отправляем команду с '\n' — сервер ждёт завершения строки */
        char cmd_with_newline[BUFFER_SIZE + 2];
        int wn = snprintf(cmd_with_newline, sizeof(cmd_with_newline),
                          "%s\n", input_cmd);
        if (wn < 0 || (size_t)wn >= sizeof(cmd_with_newline)) {
            printf("ERR Команда слишком длинная\n");
            continue;
        }

        if (send_all(sockfd, cmd_with_newline, (size_t)wn) != 0) {
            perror("Ошибка отправки команды");
            break;
        }

        if (receive_response(sockfd) != 0) {
            break; /* соединение разорвано */
        }
    }

    close(sockfd);
    printf("Соединение закрыто.\n");
    return 0;
}