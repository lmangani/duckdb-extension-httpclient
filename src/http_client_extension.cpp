#define DUCKDB_EXTENSION_MAIN
#include "http_client_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"

#include <string>
#include <sstream>

namespace duckdb {

static void HTTPGetRequestFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    D_ASSERT(args.data.size() == 1);

    UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t input) {
        std::string url = input.GetString();
        duckdb_httplib_openssl::Client client(url);

        auto res = client.Get("/");
        if (res) {
            if (res->status == 200) {
                return StringVector::AddString(result, res->body);
            } else {
                throw std::runtime_error("HTTP error: " + std::to_string(res->status));
            }
        } else {
            auto err = res.error();
            throw std::runtime_error("HTTP error: " + duckdb_httplib_openssl::to_string(err));  // Fully qualify to_string
        }
    });
}

static void HTTPPostRequestFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    D_ASSERT(args.data.size() == 3);

    auto &url_vector = args.data[0];
    auto &headers_vector = args.data[1];
    auto &body_vector = args.data[2];

    TernaryExecutor::Execute<string_t, string_t, string_t, string_t>(
        url_vector, headers_vector, body_vector, result, args.size(),
        [&](string_t url, string_t headers, string_t body) {
            std::string url_str = url.GetString();
            duckdb_httplib_openssl::Client client(url_str);

	    // HeaderMap header_map = {};

            duckdb_httplib_openssl::Headers header_map;  // Fully qualified httplib::Headers
            std::istringstream header_stream(headers.GetString());
            std::string header;
            while (std::getline(header_stream, header)) {
                size_t colon_pos = header.find(':');
                if (colon_pos != std::string::npos) {
                    std::string key = header.substr(0, colon_pos);
                    std::string value = header.substr(colon_pos + 1);
                    // Trim leading and trailing whitespace
                    key.erase(0, key.find_first_not_of(" \t"));
                    key.erase(key.find_last_not_of(" \t") + 1);
                    value.erase(0, value.find_first_not_of(" \t"));
                    value.erase(value.find_last_not_of(" \t") + 1);
                    header_map.emplace(key, value);
                }
            }

            auto res = client.Post("/", header_map, body.GetString(), "application/json");
            if (res) {
                if (res->status == 200) {
                    return StringVector::AddString(result, res->body);
                } else {
                    throw std::runtime_error("HTTP error: " + std::to_string(res->status));
                }
            } else {
                auto err = res.error();
                throw std::runtime_error("HTTP error: " + duckdb_httplib_openssl::to_string(err));  // Fully qualify to_string
            }
        });
}

static void LoadInternal(DatabaseInstance &instance) {
    ScalarFunctionSet http_get("http_get");
    http_get.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, HTTPGetRequestFunction));
    ExtensionUtil::RegisterFunction(instance, http_get);

    ScalarFunctionSet http_post("http_post");
    http_post.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
        LogicalType::VARCHAR, HTTPPostRequestFunction));
    ExtensionUtil::RegisterFunction(instance, http_post);
}

void HttpClientExtension::Load(DuckDB &db) {
    LoadInternal(*db.instance);
}

std::string HttpClientExtension::Name() {
    return "http_client";
}

std::string HttpClientExtension::Version() const {
#ifdef EXT_VERSION_HTTPCLIENT
    return EXT_VERSION_HTTPCLIENT;
#else
    return "";
#endif
}


} // namespace duckdb

extern "C" {
DUCKDB_EXTENSION_API void http_client_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::HttpClientExtension>();
}

DUCKDB_EXTENSION_API const char *http_client_version() {
    return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif

