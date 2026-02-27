/*
 * This file is part of the oGRAC project.
 * Copyright (c) 2026 Huawei Technologies Co.,Ltd.
 *
 * oGRAC is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * init_exec.c
 *
 *
 * IDENTIFICATION
 * src/driver/ogodbc/init_exec.c
 */
#include "init_exec.h"
#include "cm_charset.h"
#include "cm_file.h"
#include "cm_timer.h"

static void *dl_open;
static SQLGetPrivateProfileStringFunc sqlGetPrivateProfileString;

void load_odbc_config()
{
    void *dl_inst = NULL;
    const char *func = "SQLGetPrivateProfileString";
    const char *odbcinst = "libodbcinst.so";
    void **profile = (void **)(&sqlGetPrivateProfileString);

    if (dl_open != NULL) {
        return;
    }
    dl_inst = dlopen(odbcinst, RTLD_LAZY);
    if (dl_inst == NULL) {
        return;
    }
    *profile = dlsym(dl_inst, func);
    dl_open = dl_inst;
}

static SQLRETURN verify_conn_info(connection_class *conn, ConnInfo *ci)
{
    if (ci->username[0] == '\0' || ci->password[0] == '\0') {
        conn->err_sign = 1;
        conn->error_msg = "Username or Password is invalid.";
        return SQL_ERROR;
    }
    if (ci->server[0] == '\0') {
        conn->err_sign = 1;
        conn->error_msg = "Servername is invalid.";
        return SQL_ERROR;
    }
    if (ci->port[0] == '\0') {
        conn->err_sign = 1;
        conn->error_msg = "Port is invalid.";
        return SQL_ERROR;
    }
    return SQL_SUCCESS;
}

static status_t set_ssl_param(connection_class *conn, ConnInfo *info)
{
    status_t status;

    status = ogconn_set_conn_attr(conn->ogconn, OGCONN_ATTR_SSL_CA, info->ssl_ca, (uint32)strlen(info->ssl_ca));
    if (status != OGCONN_SUCCESS) {
        get_err(conn);
        return OG_ERROR;
    }

    status = ogconn_set_conn_attr(conn->ogconn, OGCONN_ATTR_SSL_KEY, info->ssl_key, (uint32)strlen(info->ssl_key));
    if (status != OGCONN_SUCCESS) {
        get_err(conn);
        return OG_ERROR;
    }

    status = ogconn_set_conn_attr(conn->ogconn, OGCONN_ATTR_SSL_MODE, &info->ssl_mode, sizeof(ogconn_ssl_mode_t));
    if (status != OGCONN_SUCCESS) {
        get_err(conn);
        return OG_ERROR;
    }

    status = ogconn_set_conn_attr(conn->ogconn, OGCONN_ATTR_SSL_CERT, info->ssl_cert, (uint32)strlen(info->ssl_cert));
    if (status != OGCONN_SUCCESS) {
        get_err(conn);
        return OG_ERROR;
    }

    status = ogconn_set_conn_attr(conn->ogconn, OGCONN_ATTR_SSL_CRL, info->ssl_crl, (uint32)strlen(info->ssl_crl));
    if (status != OGCONN_SUCCESS) {
        get_err(conn);
        return OG_ERROR;
    }

    status = ogconn_set_conn_attr(conn->ogconn, OGCONN_ATTR_SSL_CIPHER,
                                  info->ssl_encryption, (uint32)strlen(info->ssl_encryption));
    if (status != OGCONN_SUCCESS) {
        get_err(conn);
        return OG_ERROR;
    }

    status = ogconn_set_conn_attr(conn->ogconn, OGCONN_ATTR_UDS_SERVER_PATH,
                                  info->uds_path, (uint32)strlen(info->uds_path));
    if (status != OGCONN_SUCCESS) {
        get_err(conn);
        return OG_ERROR;
    }
    return OG_SUCCESS;
}

static status_t init_ssl_info(connection_class *conn, ConnInfo *info)
{
    status_t status;
    char ssl_encryption[SSL_ENCRYPTION_KEY_LEN];
    char str_buf[KEYPWD_BUF_LEN];
    int32 file = 0;
    int32 read_size = 0;
    uint32 ssl_encryption_len = sizeof(ssl_encryption);
    uint32 str_buf_len = sizeof(str_buf);
    uchar read_buf[AES_READ_LEN];

    status = set_ssl_param(conn, info);
    if (status != OG_SUCCESS) {
        return status;
    }
    
    if (info->sslpassword != NULL && info->sslpassword[0] != 0 && info->ssl_key != NULL && info->ssl_key[0] != 0) {
        bool32 is_file_exist = cm_file_exist(info->ssl_factory);
        status = cm_open_file_ex(info->ssl_factory, O_SYNC | O_RDONLY | O_BINARY, S_IRUSR, &file);
        if (status != OG_SUCCESS || !is_file_exist) {
            if (conn->error_msg == NULL) {
                conn->error_msg = "failed to open ssl file";
                conn->err_sign = 1;
            }
            return OG_ERROR;
        }

        if (cm_read_file(file, read_buf, AES_READ_LEN, &read_size) != OG_SUCCESS) {
            if (conn->error_msg == NULL) {
                conn->error_msg = "failed to read ssl file";
                conn->err_sign = 1;
            }
            cm_close_file(file);
            return OG_ERROR;
        }
        cm_close_file(file);

        status = cm_base64_encode((unsigned char *)read_buf, AES_READ_LEN, ssl_encryption, &ssl_encryption_len);
        if (status != OG_SUCCESS) {
            if (conn->error_msg == NULL) {
                conn->error_msg = "failed to encode ssl encryption key";
                conn->err_sign = 1;
            }
            return OG_ERROR;
        }

        status = cm_decrypt_passwd(OG_TRUE, info->sslpassword, (unsigned int)strlen(info->sslpassword),
                                   str_buf, &str_buf_len, info->ssl_key_native, ssl_encryption);
        if (status != OG_SUCCESS) {
            if (conn->error_msg == NULL) {
                conn->error_msg = "failed to decrypt ssl password";
                conn->err_sign = 1;
            }
            return OG_ERROR;
        }

        status = ogconn_set_conn_attr(conn->ogconn, OGCONN_ATTR_SSL_KEYPWD, str_buf, str_buf_len);
        if (status != OGCONN_SUCCESS) {
            get_err(conn);
            return OG_ERROR;
        }

        MEMS_RETURN_IFERR(memset_s(str_buf, KEYPWD_BUF_LEN, 0, KEYPWD_BUF_LEN));
        return OG_SUCCESS;
    }

    status = ogconn_set_conn_attr(conn->ogconn, OGCONN_ATTR_SSL_KEYPWD, "", 0);
    if (status != OGCONN_SUCCESS) {
        get_err(conn);
        return OG_ERROR;
    }
    return OG_SUCCESS;
}

SQLRETURN og_db_connect(connection_class *conn, ConnInfo *info)
{
    errno_t err;
    status_t status;
    uint32 asc_len = AES256_SIZE / 2;
    char asc_key[AES256_SIZE / 2 + 4];
    uint32 connHostlen = 0;
    uint32 serverLen = 0;
    uint32 portLen = 0;
    char connHost[MAX_CONN_HOST_LEN];
    char *ssl_encryption = info->ssl_encryption;
    char *ssl_factory = info->ssl_factory;
    uint32 ssl_factory_len = (uint32)strlen(ssl_factory);
    uint32 key_file_len = SSL_ENCRYPTION_KEY_LEN;
    char *ssl_key_native = info->ssl_key_native;
    uint32 ssl_encryption_len = OG_MAX_CIPHER_LEN;

    if (verify_conn_info(conn, info) != SQL_SUCCESS) {
        return SQL_ERROR;
    }

    serverLen = strlen(info->server);
    portLen = strlen(info->port);
    connHostlen = serverLen + portLen + 1;
    err = memcpy_s(connHost, sizeof(connHost), info->server, serverLen);
    if (err != 0) {
        conn->err_sign = 1;
        conn->error_msg = "secure C lib has throw an error.";
        return SQL_ERROR;
    }
    connHost[serverLen] = ':';
    err = memcpy_s(connHost + serverLen + 1, portLen, info->port, portLen);
    if (err != 0) {
        conn->err_sign = 1;
        conn->error_msg = "secure C lib has throw an error.";
        return SQL_ERROR;
    }

    connHost[connHostlen] = '\0';
    if (info->charset[0] != '\0') {
        int ret = ogconn_set_conn_attr(conn->ogconn, OGCONN_ATTR_CHARSET_TYPE,
                                       info->charset, strlen(info->charset));
        if (ret != 0) {
            conn->err_sign = 1;
            conn->error_msg = "set charset type failed.";
            return SQL_ERROR;
        }
    }

    if (init_ssl_info(conn, info) != OG_SUCCESS) {
        return SQL_ERROR;
    }

    status = ogconn_connect(conn->ogconn, connHost, info->username, info->password);
    if (status == 0) {
        conn->flag = 0;
    }
    if (status != OG_SUCCESS) {
        get_err(conn);
        return SQL_ERROR;
    }

    if (ssl_factory_len != MAX_SSL_KEY_LEN) {
        MEMS_RETURN_IFERR(memset_s(ssl_factory, SSL_MAX_PARAM_LEN, 0, SSL_MAX_PARAM_LEN));
        MEMS_RETURN_IFERR(memset_s(ssl_key_native, SSL_MAX_KEY_LEN, 0, SSL_MAX_KEY_LEN));
        OG_RETURN_IFERR(cm_rand((unsigned char *)asc_key, asc_len));
        OG_RETURN_IFERR(cm_base64_encode((unsigned char *)asc_key, asc_len, ssl_factory, &key_file_len));
        OG_RETURN_IFERR(cm_generate_work_key(ssl_factory, ssl_key_native, SSL_MAX_KEY_LEN));
    }

    int encrypt_rs = ogconn_encrypt_password(info->password, (uint32)strlen(info->password), ssl_key_native,
                                ssl_factory, ssl_encryption, &ssl_encryption_len);
    if (encrypt_rs != 0) {
        return SQL_ERROR;
    }
    return SQL_SUCCESS;
}

void get_err(HDBC conn)
{
    connection_class *pConn = (connection_class *)conn;
    ogconn_get_error(pConn->ogconn, &pConn->error_code, (const char **)&pConn->error_msg);
    pConn->err_sign = (pConn->error_msg) ? 1 : 0;
    if (pConn->error_code == 0) {
        pConn->error_msg = NULL;
    }
}

static char *mode_set[] = {"DISABLED", "PREFERRED", "REQUIRED", "VERIFY_CA", "VERIFY_FULL"};

SQLRETURN init_odbc_dsn(connection_class *conn, ConnInfo *info)
{
    char *INIT_DSN = info->dsn;
    uint32 index = 0;
    char ssl_mode[MAX_MODE_LEN];
    int mode_total = sizeof(mode_set) / sizeof(mode_set[0]);

    ssl_mode[0] = '\0';
    if (sqlGetPrivateProfileString == NULL) {
        conn->err_sign = 1;
        conn->error_msg = "failed to load symbol from dynamic library file";
        return SQL_ERROR;
    }

    info->ssl_mode = OGCONN_SSL_PREFERRED;
    sqlGetPrivateProfileString(INIT_DSN, SERVERNAME, "", info->server, sizeof(info->server), ODBC_INI);
    sqlGetPrivateProfileString(INIT_DSN, DATABASE, "", info->database, sizeof(info->database), ODBC_INI);
    sqlGetPrivateProfileString(INIT_DSN, USERNAME, "", info->username, sizeof(info->username), ODBC_INI);
    sqlGetPrivateProfileString(INIT_DSN, PASSWORD, "", info->password, sizeof(info->password), ODBC_INI);
    sqlGetPrivateProfileString(INIT_DSN, PORT, "", info->port, sizeof(info->port), ODBC_INI);
    sqlGetPrivateProfileString(INIT_DSN, CHARSET, "", info->charset, sizeof(info->charset), ODBC_INI);
    sqlGetPrivateProfileString(INIT_DSN, SSL_CA, "", info->ssl_ca, sizeof(info->ssl_ca), ODBC_INI);
    sqlGetPrivateProfileString(INIT_DSN, SSL_CERT, "", info->ssl_cert, sizeof(info->ssl_cert), ODBC_INI);
    sqlGetPrivateProfileString(INIT_DSN, SSL_KEY, "", info->ssl_key, sizeof(info->ssl_key), ODBC_INI);
    sqlGetPrivateProfileString(INIT_DSN, SSL_PASSWORD, "", info->sslpassword, sizeof(info->sslpassword), ODBC_INI);
    sqlGetPrivateProfileString(INIT_DSN, SSL_CRL, "", info->ssl_crl, sizeof(info->ssl_crl), ODBC_INI);
    sqlGetPrivateProfileString(INIT_DSN, SSL_ENCRYPTION, "", info->ssl_encryption,
                               sizeof(info->ssl_encryption), ODBC_INI);
    sqlGetPrivateProfileString(INIT_DSN, SSL_KEY_NATIVE, "", info->ssl_key_native,
                               sizeof(info->ssl_key_native), ODBC_INI);
    sqlGetPrivateProfileString(INIT_DSN, SSL_FACTORY, "", info->ssl_factory, sizeof(info->ssl_factory), ODBC_INI);
    sqlGetPrivateProfileString(INIT_DSN, UDS_PATH, "", info->uds_path, sizeof(info->uds_path), ODBC_INI);
    sqlGetPrivateProfileString(INIT_DSN, SSL_MODE, "", ssl_mode, sizeof(ssl_mode), ODBC_INI);

    while (index < mode_total) {
        if ((uint32)strlen(ssl_mode) != (uint32)strlen(mode_set[index])) {
            index++;
            continue;
        }

        bool32 matched = 0;
        size_t p = 0;
        while (p < (uint32)strlen(ssl_mode)) {
            if (UPPER(ssl_mode[p]) != UPPER(mode_set[index][p])) {
                matched = 1;
                break;
            }
            p++;
        }
        if (matched) {
            index++;
            continue;
        }

        info->ssl_mode = (ogconn_ssl_mode_t)index;
        break;
    }
    return SQL_SUCCESS;
}

static status_t bind_data_to_pos(statement *stmt, sql_input_data *input_data)
{
    input_data->param_stream = (char *)malloc(MAX_VALUE_BUFF_LEN);
    if (input_data->param_stream == NULL) {
        return SQL_ERROR;
    }
    if (memset_s(input_data->param_stream, MAX_VALUE_BUFF_LEN, 0, MAX_VALUE_BUFF_LEN) != 0) {
        stmt->conn->err_sign = 1;
        stmt->conn->error_msg = "secure C lib has throw an error.";
        return SQL_ERROR;
    }

    return ogconn_bind_by_pos2(stmt->ctconn_stmt, input_data->index, input_data->og_type, input_data->param_stream,
                               input_data->param_len, input_data->param_size, input_data->param_type);
}

static SQLRETURN bind_string_param(statement *stmt, sql_input_data *input_data)
{
    status_t status;
    uint16 *ind = NULL;

    if (input_data->size == NULL || *input_data->size != SQL_DATA_AT_EXEC) {
        if (input_data->size != NULL) {
            ind = input_data->param_size;
        }
        status = ogconn_bind_by_pos2(stmt->ctconn_stmt, input_data->index, input_data->og_type,
                input_data->param_value, input_data->param_len, ind, input_data->param_type);
    } else {
        status = bind_data_to_pos(stmt, input_data);
    }

    if (status != OG_SUCCESS) {
        get_err(stmt->conn);
        return SQL_ERROR;
    }
    return SQL_SUCCESS;
}
 
static SQLRETURN bind_sql_param(statement *stmt, sql_input_data *input_data)
{
    ogconn_type_t og_type = input_data->og_type;
    uint16 *ind = NULL;
    status_t status;

    if (og_type == OGCONN_TYPE_NUMBER2 || og_type == OGCONN_TYPE_NUMBER
        || og_type == OGCONN_TYPE_DECIMAL) {
        if (input_data->sql_type == SQL_C_CHAR) {
            return bind_string_param(stmt, input_data);
        } else {
            input_data->is_convert = SQL_TRUE;
            status = ogconn_bind_by_pos2(stmt->ctconn_stmt, input_data->index, input_data->og_type,
                input_data->input_param, OG_BUFF_SIZE, input_data->param_size, input_data->param_type);
        }
    } else if (og_type == OGCONN_TYPE_DATE || og_type == OGCONN_TYPE_TIMESTAMP
               || og_type == OGCONN_TYPE_TIMESTAMP_TZ || og_type == OGCONN_TYPE_TIMESTAMP_LTZ) {
        if (input_data->size != NULL) {
            ind = input_data->param_size;
        }
        if (input_data->sql_type == SQL_C_CHAR) {
            status = ogconn_bind_by_pos2(stmt->ctconn_stmt, input_data->index, OGCONN_TYPE_STRING,
                input_data->param_value, input_data->param_len, ind, input_data->param_type);
        } else {
            input_data->is_convert = SQL_TRUE;
            status = ogconn_bind_by_pos2(stmt->ctconn_stmt, input_data->index, OGCONN_TYPE_TIMESTAMP,
                input_data->input_param, OG_BUFF_SIZE, input_data->param_size, input_data->param_type);
        }
    } else {
        return bind_string_param(stmt, input_data);
    }

    if (status != OG_SUCCESS) {
        get_err(stmt->conn);
        return SQL_ERROR;
    }
    return SQL_SUCCESS;
}

SQLRETURN bind_param_by_c_type(statement *stmt, sql_input_data *input_data)
{
    SQLRETURN ret;

    ret = bind_sql_param(stmt, input_data);
    if (ret == SQL_ERROR) {
        return SQL_ERROR;
    }

    if (ret == SQL_SUCCESS) {
        if (input_data->size == NULL) {
            input_data->param_offset = input_data->param_offset + input_data->param_len;
        } else if (*(input_data->size) == SQL_NTS) {
            input_data->param_offset = strlen(input_data->param_value);
        } else if (*(input_data->size) == SQL_DATA_AT_EXEC) {
            input_data->param_offset = 0;
        } else {
            input_data->param_offset = *input_data->size;
        }
    }
    if (input_data->sql_type == SQL_C_CHAR
        || input_data->sql_type == SQL_WVARCHAR
        || input_data->sql_type == SQL_WLONGVARCHAR) {
        ogconn_sql_set_param_c_type(stmt->ctconn_stmt, input_data->index, OG_TRUE);
    }
    return ret;
}

void clean_up_bind_param(sql_input_data *input_data)
{
    if (input_data != NULL) {
        if (input_data->input_param) {
            free(input_data->input_param);
            input_data->input_param = NULL;
        }
        if (input_data->param_size) {
            free(input_data->param_size);
            input_data->param_size = NULL;
        }
        if (input_data->param_stream) {
            free(input_data->param_stream);
            input_data->param_stream = NULL;
        }
    }
}

void clean_up_param(bilist_node_t *node, bilist_t *params, sql_input_data *input_data)
{
    cm_bilist_del(node, params);
    clean_up_bind_param(input_data);
    if (input_data != NULL) {
        free(input_data);
        input_data = NULL;
    }
}

void del_stmt_param(bilist_t *params, bool8 is_init)
{
    bilist_node_t *item_param = params->head;
    bilist_node_t *next_param = NULL;
    sql_input_data *input_data = NULL;

    while (item_param != NULL) {
        input_data = (sql_input_data *)((char *)item_param - OFFSET_OF(sql_input_data, data_list));
        next_param = item_param->next;
        if (is_init) {
            if (input_data->param_flag == NOT_EXEC_PARAM || input_data->param_flag == EXEC_PARAM) {
                clean_up_param(item_param, params, input_data);
            }
        } else {
            if (input_data->param_flag == INIT_PARAM ||
                input_data->param_flag == EXEC_PARAM ||
                input_data->param_flag == FREE_PARAM) {
                clean_up_param(item_param, params, input_data);
            }
        }
        item_param = next_param;
    }
}

SQLRETURN malloc_bind_param(sql_input_data *input_data)
{
    uint64 bind_len_size = sizeof(uint16) * input_data->param_count;
    uint64 value_size = sizeof(paramData) * input_data->param_count;
    errno_t err;

    if (input_data->param_count == 0) {
        return SQL_INVALID_HANDLE;
    }

    char *value_len_buff = (char *)malloc(bind_len_size);
    if (value_len_buff == NULL) {
        return SQL_INVALID_HANDLE;
    }

    err = memset_s(value_len_buff, bind_len_size, 0, bind_len_size);
    if (err != 0) {
        free(value_len_buff);
        value_len_buff = NULL;
        return SQL_INVALID_HANDLE;
    }
    input_data->param_size = (uint16 *)value_len_buff;
    if (!input_data->is_convert) {
        return SQL_SUCCESS;
    }

    char *value_buff = (char *)malloc(value_size);
    if (value_buff == NULL) {
        return SQL_INVALID_HANDLE;
    }
    err = memset_s(value_buff, value_size, 0, value_size);
    if (err != 0) {
        free(value_buff);
        value_buff = NULL;
        return SQL_INVALID_HANDLE;
    }
    input_data->input_param = (paramData *)value_buff;
    return SQL_SUCCESS;
}

sql_input_data *generate_bind_param_instance(bilist_node_t *item_param, uint32 pos)
{
    sql_input_data *input_data = NULL;

    while (item_param != NULL) {
        input_data = (sql_input_data *)((char *)item_param - OFFSET_OF(sql_input_data, data_list));
        if (input_data->index != pos) {
            item_param = item_param->next;
            continue;
        }
        return input_data;
    }
    return NULL;
}

db_type_map type_map[] = {
    {SQL_FLOAT, OGCONN_TYPE_REAL},
    {SQL_DOUBLE, OGCONN_TYPE_REAL},
    {SQL_NUMERIC, OGCONN_TYPE_NUMBER2},
    {SQL_DECIMAL, OGCONN_TYPE_NUMBER2},
    {SQL_CHAR, OGCONN_TYPE_CHAR},
    {SQL_WCHAR, OGCONN_TYPE_CHAR},
    {SQL_VARCHAR, OGCONN_TYPE_VARCHAR},
    {SQL_WVARCHAR, OGCONN_TYPE_VARCHAR},
    {SQL_INTEGER, OGCONN_TYPE_INTEGER},
    {SQL_BIGINT, OGCONN_TYPE_BIGINT},
    {SQL_DATE, OGCONN_TYPE_DATE},
    {SQL_TIME, OGCONN_TYPE_DATE},
    {SQL_TYPE_TIMESTAMP, OGCONN_TYPE_TIMESTAMP},
    {SQL_TIMESTAMP, OGCONN_TYPE_TIMESTAMP},
    {SQL_TYPE_DATE, OGCONN_TYPE_DATE},
    {SQL_TYPE_TIME, OGCONN_TYPE_DATE},
    {SQL_BINARY, OGCONN_TYPE_BINARY},
    {SQL_VARBINARY, OGCONN_TYPE_VARBINARY},
    {SQL_LONGVARCHAR, OGCONN_TYPE_CLOB},
    {SQL_WLONGVARCHAR, OGCONN_TYPE_CLOB},
    {SQL_LONGVARBINARY, OGCONN_TYPE_BLOB}
};

const int TYPE_MAP_SIZE = sizeof(type_map) / sizeof(type_map[0]);

ogconn_type_t sql_type_map_to_db_type(SQLSMALLINT sql_type)
{
    for (int i = 0; i < TYPE_MAP_SIZE; i++) {
        if (type_map[i].sql_Type == sql_type) {
            return type_map[i].db_Type;
        }
    }
    return OGCONN_TYPE_UNKNOWN;
}

c_type_map sql_type_map[] = {
    {OGCONN_TYPE_INTEGER, SQL_C_SLONG},
    {OGCONN_TYPE_BOOLEAN, SQL_C_SLONG},
    {OGCONN_TYPE_BIGINT, SQL_C_ULONG},
    {OGCONN_TYPE_REAL, SQL_C_DOUBLE},
    {OGCONN_TYPE_NUMBER, SQL_C_NUMERIC},
    {OGCONN_TYPE_NUMBER2, SQL_C_NUMERIC},
    {OGCONN_TYPE_DECIMAL, SQL_C_NUMERIC},
    {OGCONN_TYPE_DATE, SQL_C_TYPE_TIMESTAMP},
    {OGCONN_TYPE_TIMESTAMP, SQL_C_TYPE_TIMESTAMP},
    {OGCONN_TYPE_TIMESTAMP_TZ_FAKE, SQL_C_TYPE_TIMESTAMP},
    {OGCONN_TYPE_TIMESTAMP_TZ, SQL_C_TYPE_TIMESTAMP},
    {OGCONN_TYPE_TIMESTAMP_LTZ, SQL_C_TYPE_TIMESTAMP},
    {OGCONN_TYPE_CHAR, SQL_C_CHAR},
    {OGCONN_TYPE_VARCHAR, SQL_C_CHAR},
    {OGCONN_TYPE_STRING, SQL_C_CHAR},
    {OGCONN_TYPE_CLOB, SQL_C_CHAR},
    {OGCONN_TYPE_BINARY, SQL_C_CHAR},
    {OGCONN_TYPE_VARBINARY, SQL_C_CHAR},
    {OGCONN_TYPE_BLOB, SQL_C_CHAR}
};

const int C_TYPE_MAP_SIZE = sizeof(sql_type_map) / sizeof(sql_type_map[0]);

SQLSMALLINT db_type_map_to_c_type(uint16 type)
{
    for (int i = 0; i < C_TYPE_MAP_SIZE; i++) {
        if (sql_type_map[i].db_Type == type) {
            return sql_type_map[i].sql_Type;
        }
    }
    return SQL_C_DEFAULT;
}

sql_input_data *build_bind_param(bilist_t *param_list, uint32 pos)
{
    sql_input_data *input_data = NULL;

    input_data = generate_bind_param_instance(param_list->head, pos);
    if (input_data != NULL) {
        return input_data;
    }
    
    input_data = (sql_input_data*)malloc(sizeof(sql_input_data));
    if (input_data != NULL) {
        input_data->index = pos;
        input_data->input_param = NULL;
        input_data->param_size = NULL;
        input_data->param_stream = NULL;
        cm_bilist_add_tail(&input_data->data_list, param_list);
    }
    return input_data;
}

type_size_map size_map[] = {
    {SQL_C_SBIGINT, sizeof(SQLBIGINT)},
    {SQL_C_UBIGINT, sizeof(SQLBIGINT)},
    {SQL_C_STINYINT, sizeof(SQLCHAR)},
    {SQL_C_UTINYINT, sizeof(SQLCHAR)},
    {SQL_C_SSHORT, sizeof(SQLSMALLINT)},
    {SQL_C_USHORT, sizeof(SQLSMALLINT)},
    {SQL_C_SHORT, sizeof(SQLSMALLINT)},
    {SQL_C_FLOAT, sizeof(SQLREAL)},
    {SQL_C_DOUBLE, sizeof(SQLDOUBLE)},
    {SQL_C_NUMERIC, sizeof(SQL_NUMERIC_STRUCT)},
    {SQL_C_SLONG, sizeof(SQLINTEGER)},
    {SQL_C_ULONG, sizeof(SQLINTEGER)},
    {SQL_C_LONG, sizeof(SQLINTEGER)},
    {SQL_C_TYPE_DATE, sizeof(SQL_DATE_STRUCT)},
    {SQL_C_TYPE_TIME, sizeof(SQL_TIME_STRUCT)},
    {SQL_C_TYPE_TIMESTAMP, sizeof(SQL_TIMESTAMP_STRUCT)},
    {SQL_C_BIT, sizeof(SQLCHAR)}
};

const int TYPE_SIZE_MAP_SIZE = sizeof(size_map) / sizeof(size_map[0]);

SQLULEN cal_len_of_sql_type(SQLSMALLINT sql_c_type)
{
    for (int i = 0; i < TYPE_SIZE_MAP_SIZE; i++) {
        if (size_map[i].sql_Type == sql_c_type) {
            return size_map[i].size;
        }
    }
    return 0;
}

SQLULEN cal_column_len(SQLSMALLINT ValueType,
                       SQLULEN ColumnSize,
                       SQLLEN BufferLength,
                       const SQLPOINTER ValuePtr,
                       SQLLEN *StrLen_or_IndPtr)
{
    SQLULEN size = cal_len_of_sql_type(ValueType);
    if (size > 0) {
        return size;
    }
    if (StrLen_or_IndPtr && *StrLen_or_IndPtr == SQL_NTS) {
        return (SQLULEN)strlen((const char *)ValuePtr);
    }
    return BufferLength > ColumnSize ? BufferLength : ColumnSize;
}

void close_odbc_config()
{
    og_timer_t *timer = NULL;

    if (dl_open != NULL) {
        timer = g_timer();
        cm_close_thread(&timer->thread);
        dlclose(dl_open);
        dl_open = NULL;
    }
}