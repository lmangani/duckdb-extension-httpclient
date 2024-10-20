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

// Helper function to parse URL and setup client
static std::pair<duckdb_httplib_openssl::Client, std::string> SetupHttpClient(const std::string &url) {
    std::string scheme, domain, path;
    size_t pos = url.find("://");
    std::string mod_url = url;
    if (pos != std::string::npos) {
        scheme = mod_url.substr(0, pos);
        mod_url.erase(0, pos + 3);
    }

    pos = mod_url.find("/");
    if (pos != std::string::npos) {
        domain = mod_url.substr(0, pos);
        path = mod_url.substr(pos);
    } else {
        domain = mod_url;
        path = "/";
    }

    // Create client and set a reasonable timeout (e.g., 10 seconds)
    duckdb_httplib_openssl::Client client(domain.c_str());
    client.set_read_timeout(10, 0);  // 10 seconds
    client.set_follow_location(true); // Follow redirects

    return std::make_pair(std::move(client), path);
}

static void HandleHttpError(const duckdb_httplib_openssl::Result &res, const std::string &request_type) {
    std::string err_message = "HTTP " + request_type + " request failed. ";

    switch (res.error()) {
        case duckdb_httplib_openssl::Error::Connection:
            err_message += "Connection error.";
            break;
        case duckdb_httplib_openssl::Error::BindIPAddress:
            err_message += "Failed to bind IP address.";
            break;
        case duckdb_httplib_openssl::Error::Read:
            err_message += "Error reading response.";
            break;
        case duckdb_httplib_openssl::Error::Write:
            err_message += "Error writing request.";
            break;
        case duckdb_httplib_openssl::Error::ExceedRedirectCount:
            err_message += "Too many redirects.";
            break;
        case duckdb_httplib_openssl::Error::Canceled:
            err_message += "Request was canceled.";
            break;
        case duckdb_httplib_openssl::Error::SSLConnection:
            err_message += "SSL connection failed.";
            break;
        case duckdb_httplib_openssl::Error::SSLLoadingCerts:
            err_message += "Failed to load SSL certificates.";
            break;
        case duckdb_httplib_openssl::Error::SSLServerVerification:
            err_message += "SSL server verification failed.";
            break;
        case duckdb_httplib_openssl::Error::UnsupportedMultipartBoundaryChars:
            err_message += "Unsupported characters in multipart boundary.";
            break;
        case duckdb_httplib_openssl::Error::Compression:
            err_message += "Error during compression.";
            break;
        default:
            err_message += "Unknown error.";
            break;
    }
    throw std::runtime_error(err_message);
}


static void HTTPGetRequestFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    D_ASSERT(args.data.size() == 1);

    UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t input) {
        std::string url = input.GetString();

        // Use helper to setup client and parse URL
        auto client_and_path = SetupHttpClient(url);
        auto &client = client_and_path.first;
        auto &path = client_and_path.second;

        // Make the GET request
        auto res = client.Get(path.c_str());
        if (res) {
            if (res->status == 200) {
                return StringVector::AddString(result, res->body);
            } else {
                throw std::runtime_error("HTTP GET error: " + std::to_string(res->status) + " - " + res->reason);
            }
        } else {
            // Handle errors
            HandleHttpError(res, "GET");
        }
        // Ensure a return value in case of an error
        return string_t();
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

            // Use helper to setup client and parse URL
            auto client_and_path = SetupHttpClient(url_str);
            auto &client = client_and_path.first;
            auto &path = client_and_path.second;

            // Prepare headers
            duckdb_httplib_openssl::Headers header_map;
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

            // Make the POST request with headers and body
            auto res = client.Post(path.c_str(), header_map, body.GetString(), "application/json");
            if (res) {
                if (res->status == 200) {
                    return StringVector::AddString(result, res->body);
                } else {
                    throw std::runtime_error("HTTP POST error: " + std::to_string(res->status) + " - " + res->reason);
                }
            } else {
                // Handle errors
                HandleHttpError(res, "POST");
            }
            // Ensure a return value in case of an error
            return string_t();
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

