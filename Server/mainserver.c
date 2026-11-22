#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define PORT 6767
#define RECV_BUF_SIZE 1024
#define LINE_BUF_SIZE 2048
#define MAX_NAME_LEN 128
#define MAX_LINE_LEN 1024
#define MAX_FIELDS 32
#define INITIAL_INDEX_CAP 64

typedef struct {
    long key;       /* Индекс / номер записи (начинается с 1) */
    off_t offset;   /* Байтовое смещение начала строки в текстовом файле */
    int active;     /* 1 — активная запись, 0 — удалённая или пустая */
} IndexEntry;

typedef struct {
    int data_fd;
    int is_open;
    char db_name[MAX_NAME_LEN];       /* Имя без расширения, например "demo" */
    char data_path[MAX_NAME_LEN + 8];  /* Текстовый файл данных: "demo.txt" */
    IndexEntry *entries;
    size_t count;
    size_t capacity;
} Database;

/* --- Строковые и числовые утилиты --- */
static char *skip_ws(char *s) {
    while (*s && isspace((unsigned char)*s)) {
        s++;
    }
    return s;
}

static char *trim_ws(char *s) {
    s = skip_ws(s);
    size_t len = strlen(s);
    while (len > 0 && isspace((unsigned char)s[len - 1])) {
        s[--len] = '\0';
    }
    return s;
}

/* Разрешаем только имя базы без расширений (без '.', '/' и '\') */
static int is_valid_db_name(const char *name) {
    if (!name || name[0] == '\0') {
        return 0;
    }
    size_t len = strlen(name);
    if (len >= MAX_NAME_LEN) {
        return 0;
    }
    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)name[i];
        if (c == '.' || c == '/' || c == '\\' || isspace(c)) {
            return 0;
        }
        if (!(isalnum(c) || c == '_')) {
            return 0;
        }
    }
    return 1;
}

static int is_blank_or_spaces(const char *s) {
    for (size_t i = 0; s[i] != '\0'; i++) {
        if (!isspace((unsigned char)s[i])) {
            return 0;
        }
    }
    return 1;
}

static int parse_positive_long(const char *s, long *out) {
    if (!s) {
        return -1;
    }
    char *p = skip_ws((char *)s);
    if (*p == '\0') {
        return -1;
    }
    errno = 0;
    char *end = NULL;
    long val = strtol(p, &end, 10);
    if (errno != 0 || end == p || val <= 0) {
        return -1;
    }
    end = skip_ws(end);
    if (*end != '\0') {
        return -1;
    }
    *out = val;
    return 0;
}

/* --- Надёжная запись в файл и отправка в сокет --- */
static ssize_t write_all_fd(int fd, const void *buf, size_t count) {
    size_t done = 0;
    const char *p = (const char *)buf;
    while (done < count) {
        ssize_t n = write(fd, p + done, count - done);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (n == 0) {
            return -1;
        }
        done += (size_t)n;
    }
    return (ssize_t)done;
}

static int send_all_sock(int fd, const char *msg) {
    size_t total = 0;
    size_t len = strlen(msg);
    while (total < len) {
        ssize_t n = send(fd, msg + total, len - total, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (n == 0) {
            return -1;
        }
        total += (size_t)n;
    }
    return 0;
}

static void send_reply(int client_fd, const char *reply) {
    char out[LINE_BUF_SIZE + 128];
    snprintf(out, sizeof(out), "%s\n", reply);
    send_all_sock(client_fd, out);
}

/* --- Индексная таблица в памяти (ключ -> смещение в файле) --- */
static void index_reset(Database *db) {
    free(db->entries);
    db->entries = NULL;
    db->count = 0;
    db->capacity = 0;
}

static int index_ensure_cap(Database *db, size_t needed) {
    if (needed <= db->capacity) {
        return 0;
    }
    size_t new_cap = db->capacity == 0 ? INITIAL_INDEX_CAP : db->capacity * 2;
    while (new_cap < needed) {
        new_cap *= 2;
    }
    IndexEntry *tmp = realloc(db->entries, new_cap * sizeof(IndexEntry));
    if (!tmp) {
        return -1;
    }
    db->entries = tmp;
    db->capacity = new_cap;
    return 0;
}

static IndexEntry *index_find(Database *db, long key) {
    for (size_t i = 0; i < db->count; i++) {
        if (db->entries[i].key == key) {
            return &db->entries[i];
        }
    }
    return NULL;
}

static int index_set(Database *db, long key, off_t offset, int active) {
    IndexEntry *found = index_find(db, key);
    if (found) {
        found->offset = offset;
        found->active = active;
        return 0;
    }
    if (index_ensure_cap(db, db->count + 1) != 0) {
        return -1;
    }
    db->entries[db->count].key = key;
    db->entries[db->count].offset = offset;
    db->entries[db->count].active = active;
    db->count++;
    return 0;
}

/* Построение индексной таблицы по текстовому файлу данных */
static int db_build_index(Database *db) {
    index_reset(db);
    if (lseek(db->data_fd, 0, SEEK_SET) < 0) {
        return -1;
    }

    char chunk[512];
    char line[MAX_LINE_LEN];
    size_t line_len = 0;
    off_t cur_offset = 0;
    off_t line_start_offset = 0;
    long rec_num = 1;

    while (1) {
        ssize_t n = read(db->data_fd, chunk, sizeof(chunk));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (n == 0) {
            break;
        }

        for (ssize_t i = 0; i < n; i++) {
            char c = chunk[i];
            if (line_len == 0) {
                line_start_offset = cur_offset;
            }
            cur_offset++;

            if (c == '\n') {
                line[line_len] = '\0';
                int is_active = !is_blank_or_spaces(line);
                if (index_set(db, rec_num, line_start_offset, is_active) != 0) {
                    return -1;
                }
                rec_num++;
                line_len = 0;
            } else {
                if (line_len + 1 < MAX_LINE_LEN) {
                    line[line_len++] = c;
                }
            }
        }
    }

    if (line_len > 0) {
        line[line_len] = '\0';
        int is_active = !is_blank_or_spaces(line);
        if (index_set(db, rec_num, line_start_offset, is_active) != 0) {
            return -1;
        }
    }

    return 0;
}

static void db_close_current(Database *db) {
    if (db->is_open && db->data_fd >= 0) {
        close(db->data_fd);
    }
    db->data_fd = -1;
    db->is_open = 0;
    db->db_name[0] = '\0';
    db->data_path[0] = '\0';
    index_reset(db);
}

/* Открытие существующей базы (create_only == 0) или создание новой (create_only == 1) */
static int db_open_or_create(Database *db, const char *name, int create_only, char *err_buf, size_t err_sz) {
    if (!is_valid_db_name(name)) {
        snprintf(err_buf, err_sz, "ERR Имя базы должно быть без расширений (только буквы, цифры и '_')");
        return -1;
    }

    char data_path[MAX_NAME_LEN + 8];
    snprintf(data_path, sizeof(data_path), "%s.txt", name);

    int fd = -1;
    if (create_only) {
        fd = open(data_path, O_RDWR | O_CREAT | O_EXCL, 0644);
    } else {
        fd = open(data_path, O_RDWR);
    }

    if (fd < 0) {
        if (create_only && errno == EEXIST) {
            snprintf(err_buf, err_sz, "ERR База данных '%s' уже существует", name);
        } else if (!create_only && errno == ENOENT) {
            snprintf(err_buf, err_sz, "ERR База данных '%s' не существует", name);
        } else {
            snprintf(err_buf, err_sz, "ERR Ошибка открытия базы '%s': %s", name, strerror(errno));
        }
        return -1;
    }

    db_close_current(db);
    db->data_fd = fd;
    db->is_open = 1;
    snprintf(db->db_name, MAX_NAME_LEN, "%s", name);
    snprintf(db->data_path, sizeof(db->data_path), "%s", data_path);

    if (db_build_index(db) != 0) {
        snprintf(err_buf, err_sz, "ERR Ошибка построения индекса для '%s'", name);
        db_close_current(db);
        return -1;
    }
    return 0;
}

/* Чтение строки из текстового файла по смещению из индексной таблицы */
static int read_line_at_offset(int fd, off_t offset, char *out_line, size_t out_sz) {
    if (lseek(fd, offset, SEEK_SET) < 0) {
        return -1;
    }
    size_t idx = 0;
    while (idx + 1 < out_sz) {
        char c;
        ssize_t n = read(fd, &c, 1);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (n == 0 || c == '\n') {
            break;
        }
        out_line[idx++] = c;
    }
    out_line[idx] = '\0';
    if (idx > 0 && out_line[idx - 1] == '\r') {
        out_line[idx - 1] = '\0';
    }
    return 0;
}

/* Разбор списка полей через ';' внутри VALUES (...) */
static int parse_values_payload(const char *start, char *out_normalized, size_t out_sz, char **after_close_paren) {
    const char *open_p = strchr(start, '(');
    if (!open_p) {
        return -1;
    }
    const char *close_p = strchr(open_p + 1, ')');
    if (!close_p || close_p == open_p + 1) {
        return -1;
    }

    size_t raw_len = (size_t)(close_p - (open_p + 1));
    if (raw_len >= MAX_LINE_LEN) {
        return -1;
    }

    char raw_inside[MAX_LINE_LEN];
    memcpy(raw_inside, open_p + 1, raw_len);
    raw_inside[raw_len] = '\0';

    char normalized[MAX_LINE_LEN] = {0};
    size_t norm_pos = 0;
    int field_count = 0;
    char *cur = raw_inside;

    while (1) {
        if (field_count >= MAX_FIELDS) {
            return -1;
        }
        char *sep = strchr(cur, ';');
        if (sep) {
            *sep = '\0';
        }
        char *field_val = trim_ws(cur);
        if (field_val[0] == '\0') {
            return -1;
        }

        int w = snprintf(normalized + norm_pos, sizeof(normalized) - norm_pos,
                         "%s%s", field_count > 0 ? ";" : "", field_val);
        if (w < 0 || norm_pos + (size_t)w >= sizeof(normalized)) {
            return -1;
        }
        norm_pos += (size_t)w;
        field_count++;

        if (!sep) {
            break;
        }
        cur = sep + 1;
    }

    if (strlen(normalized) >= out_sz) {
        return -1;
    }
    strcpy(out_normalized, normalized);

    if (after_close_paren) {
        *after_close_paren = (char *)(close_p + 1);
    }
    return 0;
}

static int check_db_target(Database *db, const char *target_name, char *err_buf, size_t err_sz) {
    if (!db->is_open) {
        snprintf(err_buf, err_sz, "ERR База данных не открыта");
        return -1;
    }
    if (target_name && target_name[0] != '\0') {
        if (!is_valid_db_name(target_name)) {
            snprintf(err_buf, err_sz, "ERR Имя базы должно быть без расширений");
            return -1;
        }
        if (strcmp(db->db_name, target_name) != 0) {
            snprintf(err_buf, err_sz, "ERR Открыта база '%s', а запрошена '%s'", db->db_name, target_name);
            return -1;
        }
    }
    return 0;
}

static int append_record_line(Database *db, const char *line_content, long *out_key) {
    off_t end_off = lseek(db->data_fd, 0, SEEK_END);
    if (end_off < 0) {
        return -1;
    }

    char buf[MAX_LINE_LEN + 2];
    int fmt_n = snprintf(buf, sizeof(buf), "%s\n", line_content);
    if (fmt_n < 0 || (size_t)fmt_n >= sizeof(buf)) {
        return -1;
    }

    if (write_all_fd(db->data_fd, buf, (size_t)fmt_n) < 0) {
        return -1;
    }

    long new_key = (long)db->count + 1;
    int active = !is_blank_or_spaces(line_content);
    if (index_set(db, new_key, end_off, active) != 0) {
        return -1;
    }
    if (out_key) {
        *out_key = new_key;
    }
    return 0;
}

static void handle_command(Database *db, int client_fd, char *raw_cmd) {
    char *cmd = trim_ws(raw_cmd);
    if (*cmd == '\0') {
        return;
    }

    size_t len = strlen(cmd);
    if (len > 0 && cmd[len - 1] == ';') {
        cmd[len - 1] = '\0';
        cmd = trim_ws(cmd);
    }

    char err[256];
    char resp[LINE_BUF_SIZE + 128];

    /* 1. CREATE DB <Имя_БД> */
    if (strncmp(cmd, "CREATE DB", 9) == 0 && (cmd[9] == '\0' || isspace((unsigned char)cmd[9]))) {
        char *arg = trim_ws(cmd + 9);
        if (db_open_or_create(db, arg, 1, err, sizeof(err)) == 0) {
            snprintf(resp, sizeof(resp), "OK Создана и открыта база '%s' (файл: %s)",
                     db->db_name, db->data_path);
            send_reply(client_fd, resp);
        } else {
            send_reply(client_fd, err);
        }
        return;
    }

    /* 2. OPEN <Имя_БД> — только существующая база */
    if (strncmp(cmd, "OPEN", 4) == 0 && (cmd[4] == '\0' || isspace((unsigned char)cmd[4]))) {
        char *arg = trim_ws(cmd + 4);
        if (db_open_or_create(db, arg, 0, err, sizeof(err)) == 0) {
            snprintf(resp, sizeof(resp), "OK Открыта база '%s' (записей в индексе: %zu)", db->db_name, db->count);
            send_reply(client_fd, resp);
        } else {
            send_reply(client_fd, err);
        }
        return;
    }

    /* 3. SELECT <Номер> или SELECT * */
    if (strncmp(cmd, "SELECT", 6) == 0 && (cmd[6] == '\0' || isspace((unsigned char)cmd[6]))) {
        if (check_db_target(db, NULL, err, sizeof(err)) != 0) {
            send_reply(client_fd, err);
            return;
        }
        char *arg = trim_ws(cmd + 6);
        if (strcmp(arg, "*") == 0) {
            send_reply(client_fd, "OK LIST BEGIN");
            for (size_t i = 0; i < db->count; i++) {
                if (!db->entries[i].active) {
                    continue;
                }
                char rec[MAX_LINE_LEN];
                if (read_line_at_offset(db->data_fd, db->entries[i].offset, rec, sizeof(rec)) == 0) {
                    if (!is_blank_or_spaces(rec)) {
                        snprintf(resp, sizeof(resp), "[%ld] %s", db->entries[i].key, trim_ws(rec));
                        send_reply(client_fd, resp);
                    }
                }
            }
            send_reply(client_fd, "OK LIST END");
            return;
        }

        long key = 0;
        if (parse_positive_long(arg, &key) != 0) {
            send_reply(client_fd, "ERR Неверный аргумент SELECT (ожидается номер > 0 или *)");
            return;
        }

        IndexEntry *entry = index_find(db, key);
        if (!entry || !entry->active) {
            snprintf(resp, sizeof(resp), "ERR Запись с индексом %ld не найдена или удалена", key);
            send_reply(client_fd, resp);
            return;
        }

        char rec[MAX_LINE_LEN];
        if (read_line_at_offset(db->data_fd, entry->offset, rec, sizeof(rec)) != 0) {
            send_reply(client_fd, "ERR Ошибка чтения записи через lseek/read");
            return;
        }
        snprintf(resp, sizeof(resp), "OK [%ld] %s", entry->key, trim_ws(rec));
        send_reply(client_fd, resp);
        return;
    }

    /* 4. INSERTBACK INTO <Имя_БД> VALUES (...) */
    if (strncmp(cmd, "INSERTBACK INTO", 15) == 0 && isspace((unsigned char)cmd[15])) {
        char *p = skip_ws(cmd + 15);
        char *val_kw = strstr(p, "VALUES");
        if (!val_kw) {
            send_reply(client_fd, "ERR Ожидалось ключевое слово VALUES");
            return;
        }
        char db_target[MAX_NAME_LEN];
        size_t name_len = (size_t)(val_kw - p);
        if (name_len == 0 || name_len >= sizeof(db_target)) {
            send_reply(client_fd, "ERR Некорректное имя базы");
            return;
        }
        memcpy(db_target, p, name_len);
        db_target[name_len] = '\0';

        if (check_db_target(db, trim_ws(db_target), err, sizeof(err)) != 0) {
            send_reply(client_fd, err);
            return;
        }

        char payload[MAX_LINE_LEN];
        char *tail = NULL;
        if (parse_values_payload(val_kw + 6, payload, sizeof(payload), &tail) != 0) {
            send_reply(client_fd, "ERR Некорректный формат VALUES (...)");
            return;
        }
        if (*trim_ws(tail) != '\0') {
            send_reply(client_fd, "ERR Лишние символы после VALUES (...)");
            return;
        }

        long assigned_key = 0;
        if (append_record_line(db, payload, &assigned_key) != 0) {
            send_reply(client_fd, "ERR Ошибка добавления записи в файл базы");
            return;
        }
        snprintf(resp, sizeof(resp), "OK Добавлена запись с индексом %ld", assigned_key);
        send_reply(client_fd, resp);
        return;
    }

    /* 5. INSERT INTO <Имя_БД> VALUES (...) ON <Номер_Позиции> */
    if (strncmp(cmd, "INSERT INTO", 11) == 0 && isspace((unsigned char)cmd[11])) {
        char *p = skip_ws(cmd + 11);
        char *val_kw = strstr(p, "VALUES");
        if (!val_kw) {
            send_reply(client_fd, "ERR Ожидалось ключевое слово VALUES");
            return;
        }
        char db_target[MAX_NAME_LEN];
        size_t name_len = (size_t)(val_kw - p);
        if (name_len == 0 || name_len >= sizeof(db_target)) {
            send_reply(client_fd, "ERR Некорректное имя базы");
            return;
        }
        memcpy(db_target, p, name_len);
        db_target[name_len] = '\0';

        if (check_db_target(db, trim_ws(db_target), err, sizeof(err)) != 0) {
            send_reply(client_fd, err);
            return;
        }

        char payload[MAX_LINE_LEN];
        char *tail = NULL;
        if (parse_values_payload(val_kw + 6, payload, sizeof(payload), &tail) != 0) {
            send_reply(client_fd, "ERR Некорректный формат VALUES (...)");
            return;
        }

        tail = skip_ws(tail);
        if (strncmp(tail, "ON", 2) != 0 || !isspace((unsigned char)tail[2])) {
            send_reply(client_fd, "ERR В INSERT INTO ожидалось ON <Номер_Позиции>");
            return;
        }

        long target_pos = 0;
        if (parse_positive_long(tail + 2, &target_pos) != 0) {
            send_reply(client_fd, "ERR Некорректный номер позиции после ON");
            return;
        }

        while ((long)db->count < target_pos - 1) {
            if (append_record_line(db, "", NULL) != 0) {
                send_reply(client_fd, "ERR Ошибка добавления пустых записей");
                return;
            }
        }

        if ((long)db->count < target_pos) {
            long new_key = 0;
            if (append_record_line(db, payload, &new_key) != 0) {
                send_reply(client_fd, "ERR Ошибка записи в указанную позицию");
                return;
            }
            snprintf(resp, sizeof(resp), "OK Записано в позицию %ld", new_key);
            send_reply(client_fd, resp);
            return;
        }

        IndexEntry *target_entry = index_find(db, target_pos);
        if (!target_entry) {
            send_reply(client_fd, "ERR Позиция не найдена в индексе");
            return;
        }

        char old_line[MAX_LINE_LEN];
        if (read_line_at_offset(db->data_fd, target_entry->offset, old_line, sizeof(old_line)) != 0) {
            send_reply(client_fd, "ERR Ошибка чтения текущей записи");
            return;
        }

        size_t old_len = strlen(old_line);
        size_t new_len = strlen(payload);
        if (new_len <= old_len) {
            if (lseek(db->data_fd, target_entry->offset, SEEK_SET) < 0) {
                send_reply(client_fd, "ERR Ошибка позиционирования");
                return;
            }
            char patch[MAX_LINE_LEN];
            memset(patch, ' ', old_len);
            memcpy(patch, payload, new_len);
            patch[old_len] = '\n';

            if (write_all_fd(db->data_fd, patch, old_len + 1) < 0) {
                send_reply(client_fd, "ERR Ошибка перезаписи записи");
                return;
            }
            target_entry->active = 1;
            snprintf(resp, sizeof(resp), "OK Обновлена позиция %ld", target_pos);
            send_reply(client_fd, resp);
        } else {
            off_t end_off = lseek(db->data_fd, 0, SEEK_END);
            if (end_off < 0) {
                send_reply(client_fd, "ERR Ошибка перехода в конец файла");
                return;
            }
            char buf[MAX_LINE_LEN + 2];
            int fmt_n = snprintf(buf, sizeof(buf), "%s\n", payload);
            if (fmt_n < 0 || (size_t)fmt_n >= sizeof(buf)) {
                send_reply(client_fd, "ERR Слишком длинная запись");
                return;
            }
            if (write_all_fd(db->data_fd, buf, (size_t)fmt_n) < 0) {
                send_reply(client_fd, "ERR Ошибка записи новой строки");
                return;
            }
            if (index_set(db, target_pos, end_off, 1) != 0) {
                send_reply(client_fd, "ERR Ошибка обновления индекса");
                return;
            }
            snprintf(resp, sizeof(resp), "OK Обновлена позиция %ld (новый адрес %lld)", target_pos, (long long)end_off);
            send_reply(client_fd, resp);
        }
        return;
    }

    /* 6. DELETE FROM <Имя_БД> NUMBER <Номер_Строки> */
    if (strncmp(cmd, "DELETE FROM", 11) == 0 && isspace((unsigned char)cmd[11])) {
        char *p = skip_ws(cmd + 11);
        char *num_kw = strstr(p, "NUMBER");
        if (!num_kw) {
            send_reply(client_fd, "ERR Ожидалось ключевое слово NUMBER");
            return;
        }
        char db_target[MAX_NAME_LEN];
        size_t name_len = (size_t)(num_kw - p);
        if (name_len == 0 || name_len >= sizeof(db_target)) {
            send_reply(client_fd, "ERR Некорректное имя базы");
            return;
        }
        memcpy(db_target, p, name_len);
        db_target[name_len] = '\0';

        if (check_db_target(db, trim_ws(db_target), err, sizeof(err)) != 0) {
            send_reply(client_fd, err);
            return;
        }

        long del_key = 0;
        if (parse_positive_long(num_kw + 6, &del_key) != 0) {
            send_reply(client_fd, "ERR Некорректный номер строки после NUMBER");
            return;
        }

        IndexEntry *entry = index_find(db, del_key);
        if (!entry || !entry->active) {
            snprintf(resp, sizeof(resp), "ERR Запись %ld не найдена или уже удалена", del_key);
            send_reply(client_fd, resp);
            return;
        }

        char old_line[MAX_LINE_LEN];
        if (read_line_at_offset(db->data_fd, entry->offset, old_line, sizeof(old_line)) != 0) {
            send_reply(client_fd, "ERR Ошибка чтения записи перед удалением");
            return;
        }

        size_t line_len = strlen(old_line);
        if (line_len > 0) {
            if (lseek(db->data_fd, entry->offset, SEEK_SET) < 0) {
                send_reply(client_fd, "ERR Ошибка позиционирования при удалении");
                return;
            }
            char spaces[MAX_LINE_LEN];
            memset(spaces, ' ', line_len);
            if (write_all_fd(db->data_fd, spaces, line_len) < 0) {
                send_reply(client_fd, "ERR Ошибка очистки строки при удалении");
                return;
            }
        }
        entry->active = 0;

        snprintf(resp, sizeof(resp), "OK Удалена запись с индексом %ld", del_key);
        send_reply(client_fd, resp);
        return;
    }

    send_reply(client_fd, "ERR Неизвестная или некорректная команда");
}

int main(void) {
    int server_fd, client_fd;
    char recv_buf[RECV_BUF_SIZE];
    char client_ip[INET_ADDRSTRLEN];
    struct sockaddr_in server_addr, client_addr;
    socklen_t addr_len;

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("Ошибка создания сокета");
        exit(EXIT_FAILURE);
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT);

    if (bind(server_fd, (const struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("Ошибка bind");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 5) < 0) {
        perror("Ошибка listen");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    printf("TCP-сервер запущен и ждет подключений на порту %d...\n", PORT);

    Database db = {
        .data_fd = -1,
        .is_open = 0,
        .db_name = {0},
        .data_path = {0},
        .entries = NULL,
        .count = 0,
        .capacity = 0
    };

    while (1) {
        addr_len = sizeof(client_addr);
        client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &addr_len);
        if (client_fd < 0) {
            perror("Ошибка accept");
            continue;
        }

        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
        int client_port = ntohs(client_addr.sin_port);
        printf("[+] Подключился клиент %s:%d\n", client_ip, client_port);

        char stream_buf[LINE_BUF_SIZE * 2];
        size_t stream_len = 0;

        while (1) {
            ssize_t bytes_received = recv(client_fd, recv_buf, RECV_BUF_SIZE, 0);
            if (bytes_received < 0) {
                if (errno == EINTR) {
                    continue;
                }
                perror("Ошибка recv");
                break;
            }
            if (bytes_received == 0) {
                printf("[-] Клиент %s:%d отключился\n", client_ip, client_port);
                break;
            }

            if (stream_len + (size_t)bytes_received >= sizeof(stream_buf)) {
                send_reply(client_fd, "ERR Переполнение входного буфера запроса");
                stream_len = 0;
                continue;
            }

            memcpy(stream_buf + stream_len, recv_buf, (size_t)bytes_received);
            stream_len += (size_t)bytes_received;
            stream_buf[stream_len] = '\0';

            char *line_start = stream_buf;
            char *nl = NULL;
            while ((nl = strchr(line_start, '\n')) != NULL) {
                *nl = '\0';
                printf("[%s:%d] Команда: %s\n", client_ip, client_port, line_start);
                handle_command(&db, client_fd, line_start);
                line_start = nl + 1;
            }

            size_t remaining = (size_t)((stream_buf + stream_len) - line_start);
            memmove(stream_buf, line_start, remaining);
            stream_len = remaining;
            stream_buf[stream_len] = '\0';
        }

        close(client_fd);
    }

    db_close_current(&db);
    close(server_fd);
    return 0;
}