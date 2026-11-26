#define _POSIX_C_SOURCE 200809L

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define PORT 6767
#define BUF_SZ 4096
#define LINE_SZ 2048
#define NAME_SZ 128

typedef struct {
    long key;
    off_t offset;
    int active;
} Entry;

typedef struct {
    int fd;
    int is_open;
    char name[NAME_SZ];
    Entry *idx;
    size_t count;
    size_t cap;
} DB;

static char *skip_ws(char *s) {
    while (*s && isspace((unsigned char)*s)) {
        s++;
    }
    return s;
}

static char *trim_ws(char *s) {
    s = skip_ws(s);
    size_t n = strlen(s);
    while (n > 0 && isspace((unsigned char)s[n - 1])) {
        s[--n] = '\0';
    }
    return s;
}

static int is_blank(const char *s) {
    for (size_t i = 0; s[i]; i++) {
        if (!isspace((unsigned char)s[i])) {
            return 0;
        }
    }
    return 1;
}

static int valid_name(const char *s) {
    if (!s || !*s || strlen(s) >= NAME_SZ) {
        return 0;
    }
    for (size_t i = 0; s[i]; i++) {
        unsigned char c = (unsigned char)s[i];
        if (!(isalnum(c) || c == '_')) {
            return 0;
        }
    }
    return 1;
}

static int parse_pos_long(const char *s, long *out) {
    char *p = skip_ws((char *)s);
    if (!*p) {
        return -1;
    }

    errno = 0;
    char *end = NULL;
    long v = strtol(p, &end, 10);
    end = skip_ws(end);

    if (errno || end == p || v <= 0 || *end) {
        return -1;
    }

    *out = v;
    return 0;
}

static int write_all(int fd, const void *b, size_t n) {
    const char *p = (const char *)b;
    while (n > 0) {
        ssize_t w = write(fd, p, n);
        if (w < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        p += w;
        n -= (size_t)w;
    }
    return 0;
}

static int reply(int fd, const char *msg) {
    char b[LINE_SZ + 64];
    int n = snprintf(b, sizeof(b), "%s\n", msg);
    if (n < 0 || write_all(fd, b, (size_t)n) != 0) {
        return -1;
    }
    return 0;
}

static Entry *idx_find(DB *db, long key) {
    for (size_t i = 0; i < db->count; i++) {
        if (db->idx[i].key == key) {
            return &db->idx[i];
        }
    }
    return NULL;
}

static int idx_set(DB *db, long key, off_t off, int active) {
    Entry *e = idx_find(db, key);
    if (e) {
        e->offset = off;
        e->active = active;
        return 0;
    }

    if (db->count == db->cap) {
        size_t nc = db->cap ? db->cap * 2 : 32;
        Entry *p = (Entry *)realloc(db->idx, nc * sizeof(Entry));
        if (!p) {
            return -1;
        }
        db->idx = p;
        db->cap = nc;
    }

    db->idx[db->count++] = (Entry){key, off, active};
    return 0;
}

static int save_file(DB *db) {
    off_t end = lseek(db->fd, 0, SEEK_END);
    if (end < 0) {
        return -1;
    }

    off_t min_off = end;
    for (size_t i = 0; i < db->count; i++) {
        if (db->idx[i].offset < min_off) {
            min_off = db->idx[i].offset;
        }
    }

    size_t dsz = (end > min_off) ? (size_t)(end - min_off) : 0;
    char *data = dsz ? (char *)malloc(dsz) : NULL;
    if (dsz && !data) {
        return -1;
    }

    if (dsz) {
        if (lseek(db->fd, min_off, SEEK_SET) < 0) {
            free(data);
            return -1;
        }
        size_t got = 0;
        while (got < dsz) {
            ssize_t r = read(db->fd, data + got, dsz - got);
            if (r < 0) {
                if (errno == EINTR) {
                    continue;
                }
                free(data);
                return -1;
            }
            if (!r) {
                break;
            }
            got += (size_t)r;
        }
        dsz = got;
    }

    size_t ilen = 0;
    char *ibuf = (char *)malloc(db->count * 64 + 32);
    if (!ibuf) {
        free(data);
        return -1;
    }

    for (size_t i = 0; i < db->count; i++) {
        ilen += (size_t)snprintf(
            ibuf + ilen,
            64,
            "%ld:%lld:%d\n",
            db->idx[i].key,
            (long long)db->idx[i].offset,
            db->idx[i].active
        );
    }
    ilen += (size_t)snprintf(ibuf + ilen, 16, "---\n");

    off_t delta = (off_t)ilen - min_off;
    for (size_t i = 0; i < db->count; i++) {
        db->idx[i].offset += delta;
    }

    ilen = 0;
    for (size_t i = 0; i < db->count; i++) {
        ilen += (size_t)snprintf(
            ibuf + ilen,
            64,
            "%ld:%lld:%d\n",
            db->idx[i].key,
            (long long)db->idx[i].offset,
            db->idx[i].active
        );
    }
    ilen += (size_t)snprintf(ibuf + ilen, 16, "---\n");

    int ok = (lseek(db->fd, 0, SEEK_SET) == 0 &&
              ftruncate(db->fd, 0) == 0 &&
              write_all(db->fd, ibuf, ilen) == 0 &&
              (!dsz || write_all(db->fd, data, dsz) == 0));

    free(ibuf);
    free(data);
    return ok ? 0 : -1;
}

static int read_line_at(int fd, off_t off, char *out, size_t sz) {
    if (lseek(fd, off, SEEK_SET) < 0) {
        return -1;
    }

    size_t i = 0;
    while (i + 1 < sz) {
        char c;
        ssize_t r = read(fd, &c, 1);
        if (r < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (r <= 0 || c == '\n') {
            break;
        }
        out[i++] = c;
    }

    if (i > 0 && out[i - 1] == '\r') {
        i--;
    }
    out[i] = '\0';
    return 0;
}

static int load_index_syscalls(DB *db) {
    off_t end = lseek(db->fd, 0, SEEK_END);
    if (end < 0) {
        return -1;
    }
    if (end == 0) {
        return save_file(db);
    }
    if (lseek(db->fd, 0, SEEK_SET) < 0) {
        return -1;
    }

    char line[LINE_SZ];
    size_t idx = 0;
    int sep = 0;

    while (1) {
        char c;
        ssize_t r = read(db->fd, &c, 1);
        if (r < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (r == 0 || c == '\n') {
            line[idx] = '\0';
            if (idx > 0 && line[idx - 1] == '\r') {
                line[--idx] = '\0';
            }
            char *t = trim_ws(line);
            if (strcmp(t, "---") == 0) {
                sep = 1;
                break;
            }
            long k;
            long long o;
            int a;
            if (*t && sscanf(t, "%ld:%lld:%d", &k, &o, &a) == 3) {
                idx_set(db, k, (off_t)o, a ? 1 : 0);
            }
            idx = 0;
            if (r == 0) {
                break;
            }
        } else if (idx + 1 < sizeof(line)) {
            line[idx++] = c;
        }
    }

    return sep ? 0 : -1;
}

static int open_db(DB *db, const char *name, int create_only, char *err, size_t esz) {
    if (!valid_name(name)) {
        snprintf(err, esz, "ERR Некорректное имя БД");
        return -1;
    }

    char path[NAME_SZ + 8];
    snprintf(path, sizeof(path), "%s.txt", name);

    int flags = create_only ? (O_RDWR | O_CREAT | O_EXCL) : O_RDWR;
    int fd = open(path, flags, 0644);
    if (fd < 0) {
        snprintf(err, esz, "ERR %s", strerror(errno));
        return -1;
    }

    if (db->fd >= 0) {
        close(db->fd);
    }
    free(db->idx);
    memset(db, 0, sizeof(*db));
    db->fd = fd;
    db->is_open = 1;
    snprintf(db->name, sizeof(db->name), "%s", name);

    if (load_index_syscalls(db) != 0) {
        close(db->fd);
        db->fd = -1;
        db->is_open = 0;
        snprintf(err, esz, "ERR Ошибка чтения файла БД");
        return -1;
    }

    return 0;
}

static int parse_vals(const char *s, char *out, size_t sz, char **tail) {
    const char *p = skip_ws((char *)s);
    if (*p++ != '(') {
        return -1;
    }

    const char *cp = strchr(p, ')');
    if (!cp || (size_t)(cp - p) >= LINE_SZ) {
        return -1;
    }

    char tmp[LINE_SZ];
    memcpy(tmp, p, (size_t)(cp - p));
    tmp[cp - p] = '\0';
    out[0] = '\0';

    int cnt = 0;
    for (char *cur = tmp; ; ) {
        char *sc = strchr(cur, ';');
        if (sc) {
            *sc = '\0';
        }
        char *f = trim_ws(cur);
        if (!*f) {
            return -1;
        }
        if (cnt++ > 0 && strlen(out) + 1 < sz) {
            strcat(out, ";");
        }
        if (strlen(out) + strlen(f) >= sz) {
            return -1;
        }
        strcat(out, f);
        if (!sc) {
            break;
        }
        cur = sc + 1;
    }

    *tail = (char *)(cp + 1);
    return 0;
}

static int append_rec(DB *db, const char *val, long *key_out) {
    off_t off = lseek(db->fd, 0, SEEK_END);
    char b[LINE_SZ];
    int n = snprintf(b, sizeof(b), "%s\n", val);
    if (off < 0 || n < 0 || write_all(db->fd, b, (size_t)n) != 0) {
        return -1;
    }

    long k = (long)(db->count + 1);
    if (idx_set(db, k, off, is_blank(val) ? 0 : 1) != 0 || save_file(db) != 0) {
        return -1;
    }

    *key_out = k;
    return 0;
}

static int handle_cmd(DB *db, int cfd, char *raw) {
    char *cmd = trim_ws(raw);
    size_t l = strlen(cmd);
    while (l > 0 && cmd[l - 1] == ';') {
        cmd[--l] = '\0';
        cmd = trim_ws(cmd);
        l = strlen(cmd);
    }

    if (!*cmd) {
        return 0;
    }

    char err[256];
    char ok[256];

    if (strncmp(cmd, "CREATE DB ", 10) == 0) {
        if (open_db(db, trim_ws(cmd + 10), 1, err, sizeof(err)) != 0) {
            return reply(cfd, err);
        }
        snprintf(ok, sizeof(ok), "OK DB created and opened: %s", db->name);
        return reply(cfd, ok);
    }

    if (strncmp(cmd, "OPEN ", 5) == 0) {
        if (open_db(db, trim_ws(cmd + 5), 0, err, sizeof(err)) != 0) {
            return reply(cfd, err);
        }
        snprintf(ok, sizeof(ok), "OK DB opened: %s (records=%zu)", db->name, db->count);
        return reply(cfd, ok);
    }

    if (!db->is_open) {
        return reply(cfd, "ERR Нет открытой БД");
    }

    if (strncmp(cmd, "SELECT", 6) == 0 && (!cmd[6] || isspace((unsigned char)cmd[6]))) {
        char *arg = trim_ws(cmd + 6);
        if (strcmp(arg, "*") == 0) {
            reply(cfd, "OK LIST BEGIN");
            for (size_t i = 0; i < db->count; i++) {
                if (db->idx[i].active) {
                    char line[LINE_SZ];
                    char out[LINE_SZ + 64];
                    if (read_line_at(db->fd, db->idx[i].offset, line, sizeof(line)) == 0 && !is_blank(line)) {
                        snprintf(out, sizeof(out), "%ld: %s", db->idx[i].key, trim_ws(line));
                        reply(cfd, out);
                    }
                }
            }
            return reply(cfd, "OK LIST END");
        }

        long k;
        if (parse_pos_long(arg, &k) != 0) {
            return reply(cfd, "ERR Некорректный SELECT");
        }

        Entry *e = idx_find(db, k);
        char line[LINE_SZ];
        char out[LINE_SZ + 64];
        if (!e || !e->active || read_line_at(db->fd, e->offset, line, sizeof(line)) != 0 || is_blank(line)) {
            return reply(cfd, "ERR Запись не найдена");
        }

        snprintf(out, sizeof(out), "OK %ld: %s", e->key, trim_ws(line));
        return reply(cfd, out);
    }

    if (strncmp(cmd, "INSERTBACK INTO ", 16) == 0) {
        char *p = skip_ws(cmd + 16);
        char *vk = strstr(p, "VALUES");
        if (!vk || vk == p) {
            return reply(cfd, "ERR Синтаксис INSERTBACK");
        }

        char target[NAME_SZ];
        size_t nlen = (size_t)(vk - p);
        if (nlen >= NAME_SZ) {
            return reply(cfd, "ERR Длинное имя");
        }
        memcpy(target, p, nlen);
        target[nlen] = '\0';

        if (strcmp(trim_ws(target), db->name) != 0) {
            return reply(cfd, "ERR Неверная БД");
        }

        char norm[LINE_SZ];
        char *tail;
        if (parse_vals(vk + 6, norm, sizeof(norm), &tail) != 0 || *trim_ws(tail)) {
            return reply(cfd, "ERR VALUES");
        }

        long k;
        if (append_rec(db, norm, &k) != 0) {
            return reply(cfd, "ERR Запись");
        }

        snprintf(ok, sizeof(ok), "OK INSERTED key=%ld", k);
        return reply(cfd, ok);
    }

    if (strncmp(cmd, "INSERT INTO ", 12) == 0) {
        char *p = skip_ws(cmd + 12);
        char *vk = strstr(p, "VALUES");
        if (!vk || vk == p) {
            return reply(cfd, "ERR Синтаксис INSERT");
        }

        char target[NAME_SZ];
        size_t nlen = (size_t)(vk - p);
        if (nlen >= NAME_SZ) {
            return reply(cfd, "ERR Длинное имя");
        }
        memcpy(target, p, nlen);
        target[nlen] = '\0';

        if (strcmp(trim_ws(target), db->name) != 0) {
            return reply(cfd, "ERR Неверная БД");
        }

        char norm[LINE_SZ];
        char *tail;
        if (parse_vals(vk + 6, norm, sizeof(norm), &tail) != 0) {
            return reply(cfd, "ERR VALUES");
        }

        tail = skip_ws(tail);
        long pos;
        if (strncmp(tail, "ON", 2) != 0 || parse_pos_long(tail + 2, &pos) != 0) {
            return reply(cfd, "ERR ON <pos>");
        }

        long dk;
        while ((long)db->count < pos - 1) {
            if (append_rec(db, "", &dk) != 0) {
                return reply(cfd, "ERR Расширение");
            }
        }

        Entry *e = idx_find(db, pos);
        if (!e) {
            if (append_rec(db, norm, &dk) != 0) {
                return reply(cfd, "ERR Вставка");
            }
        } else {
            char old[LINE_SZ];
            if (read_line_at(db->fd, e->offset, old, sizeof(old)) != 0) {
                return reply(cfd, "ERR Чтение");
            }
            size_t nl = strlen(norm);
            size_t ol = strlen(old);
            if (nl <= ol) {
                char pad[LINE_SZ];
                memcpy(pad, norm, nl);
                for (size_t i = nl; i < ol; i++) {
                    pad[i] = ' ';
                }
                pad[ol] = '\n';
                if (lseek(db->fd, e->offset, SEEK_SET) < 0 ||
                    write_all(db->fd, pad, ol + 1) != 0 ||
                    idx_set(db, pos, e->offset, 1) != 0 ||
                    save_file(db) != 0) {
                    return reply(cfd, "ERR Перезапись");
                }
            } else {
                off_t end = lseek(db->fd, 0, SEEK_END);
                char b[LINE_SZ];
                int n = snprintf(b, sizeof(b), "%s\n", norm);
                if (end < 0 || n < 0 ||
                    write_all(db->fd, b, (size_t)n) != 0 ||
                    idx_set(db, pos, end, 1) != 0 ||
                    save_file(db) != 0) {
                    return reply(cfd, "ERR Перенос");
                }
            }
        }

        snprintf(ok, sizeof(ok), "OK INSERTED key=%ld", pos);
        return reply(cfd, ok);
    }

    if (strncmp(cmd, "DELETE FROM ", 12) == 0) {
        char *p = skip_ws(cmd + 12);
        char *nk = strstr(p, "NUMBER");
        if (!nk || nk == p) {
            return reply(cfd, "ERR Синтаксис DELETE");
        }

        char target[NAME_SZ];
        size_t nlen = (size_t)(nk - p);
        if (nlen >= NAME_SZ) {
            return reply(cfd, "ERR Длинное имя");
        }
        memcpy(target, p, nlen);
        target[nlen] = '\0';

        if (strcmp(trim_ws(target), db->name) != 0) {
            return reply(cfd, "ERR Неверная БД");
        }

        long k;
        if (parse_pos_long(nk + 6, &k) != 0) {
            return reply(cfd, "ERR NUMBER");
        }

        Entry *e = idx_find(db, k);
        char old[LINE_SZ];
        if (!e || !e->active || read_line_at(db->fd, e->offset, old, sizeof(old)) != 0) {
            return reply(cfd, "ERR Запись");
        }

        size_t ol = strlen(old);
        char bl[LINE_SZ];
        memset(bl, ' ', ol);
        bl[ol] = '\n';

        if (lseek(db->fd, e->offset, SEEK_SET) < 0 ||
            write_all(db->fd, bl, ol + 1) != 0 ||
            idx_set(db, k, e->offset, 0) != 0 ||
            save_file(db) != 0) {
            return reply(cfd, "ERR Удаление");
        }

        snprintf(ok, sizeof(ok), "OK DELETED key=%ld", k);
        return reply(cfd, ok);
    }

    return reply(cfd, "ERR Неизвестная команда");
}

int main(void) {
    int sfd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_addr.s_addr = htonl(INADDR_ANY),
        .sin_port = htons(PORT)
    };

    if (bind(sfd, (struct sockaddr *)&addr, sizeof(addr)) < 0 || listen(sfd, 16) < 0) {
        return 1;
    }

    printf("TCP DB server on port %d...\n", PORT);

    DB db = {.fd = -1};

    while (1) {
        int cfd = accept(sfd, NULL, NULL);
        if (cfd < 0) {
            continue;
        }

        char buf[BUF_SZ];
        size_t used = 0;

        while (1) {
            ssize_t n = recv(cfd, buf + used, sizeof(buf) - 1 - used, 0);
            if (n <= 0) {
                break;
            }

            used += (size_t)n;
            buf[used] = '\0';

            char *start = buf;
            char *nl;
            while ((nl = strchr(start, '\n')) != NULL) {
                *nl = '\0';
                handle_cmd(&db, cfd, start);
                start = nl + 1;
            }

            size_t rem = (size_t)((buf + used) - start);
            if (rem > 0) {
                memmove(buf, start, rem);
            }
            used = rem;
        }

        close(cfd);
    }
}