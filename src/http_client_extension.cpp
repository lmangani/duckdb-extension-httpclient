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

        // Parse the URL to extract the domain and path
        std::string scheme, domain, path;
        size_t pos = url.find("://");
        if (pos != std::string::npos) {
            scheme = url.substr(0, pos);
            url.erase(0, pos + 3);
        }

        pos = url.find("/");
        if (pos != std::string::npos) {
            domain = url.substr(0, pos);
            path = url.substr(pos);
        } else {
            domain = url;
            path = "/";
        }

        // Create client and set a reasonable timeout (e.g., 10 seconds)
        duckdb_httplib_openssl::Client client(domain.c_str());
        client.set_read_timeout(10, 0);  // 10 seconds

        // Follow redirects, limit to 10 by default
        client.set_follow_location(true);

        // Make the GET request
        auto res = client.Get(path.c_str());
        if (res) {
            if (res->status == 200) {
                return StringVector::AddString(result, res->body);
            } else {
                throw std::runtime_error("HTTP error: " + std::to_string(res->status) + " - " + res->reason);
            }
        } else {
            // Handle the error case
            std::string err_message = "HTTP request failed. ";

            // Convert httplib error codes to a descriptive message
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
    });
}

static void HTTPPostRequestFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    D_ASSERT(args.data.size() == 3);

    auto &url_vector = args.data[0];
    auto &headers_vector = args.data[1];  // Already passed as a serialized string
    auto &body_vector = args.data[2];     // Already passed as a JSON string

    // Use TernaryExecutor instead of UnaryExecutor
    TernaryExecutor::Execute<string_t, string_t, string_t, string_t>(
        url_vector, headers_vector, body_vector, result, args.size(),
        [&](string_t url, string_t headers_varchar, string_t body_varchar) {
            std::string url_str = url.GetString();

            // Parse the URL to extract the domain and path
            std::string scheme, domain, path;
            size_t pos = url_str.find("://");
            if (pos != std::string::npos) {
                scheme = url_str.substr(0, pos);
                url_str.erase(0, pos + 3);
            }

            pos = url_str.find("/");
            if (pos != std::string::npos) {
                domain = url_str.substr(0, pos);
                path = url_str.substr(pos);
            } else {
                domain = url_str;
                path = "/";
            }

            // Create the client and set a timeout
            duckdb_httplib_openssl::Client client(domain.c_str());
            client.set_read_timeout(10, 0);  // 10 seconds

            // Follow redirects for POST as well
            client.set_follow_location(true);

            // Deserialize the header string into a header map
            duckdb_httplib_openssl::Headers header_map;
            std::istringstream header_stream(headers_varchar.GetString());
            std::string header;
            while (std::getline(header_stream, header)) {
                size_t colon_pos = header.find(':');
                if (colon_pos != std::string::npos) {
                    std::string key = header.substr(0, colon_pos);
                    std::string value = header.substr(colon_pos + 1);
                    // Trim leading/trailing whitespace
                    key.erase(0, key.find_first_not_of(" \t"));
                    key.erase(key.find_last_not_of(" \t") + 1);
                    value.erase(0, value.find_first_not_of(" \t"));
                    value.erase(value.find_last_not_of(" \t") + 1);
                    header_map.emplace(key, value);
                }
            }

            // Prepare the POST body (it is passed as a string)
            std::string body_str = body_varchar.GetString();

            // Make the POST request
            auto res = client.Post(path.c_str(), header_map, body_str, "application/json");
            if (res) {
                if (res->status == 200) {
                    return StringVector::AddString(result, res->body);
                } else {
                    throw std::runtime_error("HTTP error: " + std::to_string(res->status) + " - " + res->reason);
                }
            } else {
                // Handle the error case
                std::string err_message = "HTTP POST request failed. ";
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

