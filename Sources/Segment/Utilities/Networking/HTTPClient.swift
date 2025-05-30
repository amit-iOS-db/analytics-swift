//
//  HTTPClient.swift
//  Segment-Tests
//
//  Created by Cody Garvin on 12/28/20.
//

import Foundation
#if os(Linux) || os(Windows)
import FoundationNetworking
#endif

public enum HTTPClientErrors: Error {
    case badSession
    case failedToOpenBatch
    case statusCode(code: Int)
    case unknown(error: Error)
}

public class HTTPClient {
    ///default segment hosts are commented
    private static let defaultAPIHost = "api.segment.io/v1"
    private static let defaultCDNHost = "cdn-settings.segment.com/v1"

    internal var session: any HTTPSession
    private var apiHost: String
    private var apiKey: String
    private var cdnHost: String

    private weak var analytics: Analytics?

    init(analytics: Analytics) {
        self.analytics = analytics

        self.apiKey = analytics.configuration.values.writeKey
        self.apiHost = analytics.configuration.values.apiHost
        self.cdnHost = analytics.configuration.values.cdnHost
        
        self.session = analytics.configuration.values.httpSession()
    }

    func segmentURL(for host: String, path: String) -> URL? {
        let s = "https://\(host)\(path)"
        let result = URL(string: s)
        return result
    }


    /// Starts an upload of events. Responds appropriately if successful or not. If not, lets the respondant
    /// know if the task should be retried or not based on the response.
    /// - Parameters:
    ///   - key: The write key the events are assocaited with.
    ///   - batch: The array of the events, considered a batch of events.
    ///   - completion: The closure executed when done. Passes if the task should be retried or not if failed.
    @discardableResult
    func startBatchUpload(writeKey: String, batch: URL, completion: @escaping (_ result: Result<Bool, Error>) -> Void) -> (any DataTask)? {
        guard let uploadURL = segmentURL(for: apiHost, path: "/b") else {
            self.analytics?.reportInternalError(HTTPClientErrors.failedToOpenBatch)
            completion(.failure(HTTPClientErrors.failedToOpenBatch))
            return nil
        }

        
        do {
            let fileData = try Data(contentsOf: batch)
            
            if let jsonObject = try JSONSerialization.jsonObject(with: fileData, options: []) as? [String: Any] {
                if let array = jsonObject["batch"] as? [[String: Any]] {
                    if array.count == 0 {
                        analytics?.reportInternalError(AnalyticsError.networkServerLimited("Internal: batch count is zero for url: \(batch) and file data count: \(fileData.count), file string: \(String(describing: String(data: fileData, encoding: .utf8)))", 400))
                    }
                } else {
                    analytics?.reportInternalError(AnalyticsError.networkServerLimited("Internal: not able to find batch in json for url: \(batch) and file data count: \(fileData.count), file string: \(String(describing: String(data: fileData, encoding: .utf8)))", 400))
                }
            } else {
                analytics?.reportInternalError(AnalyticsError.networkServerLimited("Internal: Unable to parse json from file data for url: \(batch) and file data count: \(fileData.count), file string: \(String(describing: String(data: fileData, encoding: .utf8)))", 400))
            }

        } catch {
            analytics?.reportInternalError(AnalyticsError.networkServerLimited("Internal: Error while reading file from url: \(batch)", 400))
        }
                
        let urlRequest = configuredRequest(for: uploadURL, method: "POST")
        
        let dataTask = session.uploadTask(with: urlRequest, fromFile: batch) { [weak self] (data, response, error) in
            guard let self else { return }
            
            print("\nAPI Request as curl \n \(urlRequest.cURLRepresentation())")

            if let data {
                print("Segment API Response: \(String(describing: String(data: data, encoding: .utf8)))")
            }
            handleResponse(data: data, response: response, error: error, url: uploadURL, batch: batch, completion: completion)
        }

        dataTask.resume()
        return dataTask
    }
    
    /// Starts an upload of events. Responds appropriately if successful or not. If not, lets the respondant
    /// know if the task should be retried or not based on the response.
    /// - Parameters:
    ///   - key: The write key the events are assocaited with.
    ///   - batch: The array of the events, considered a batch of events.
    ///   - completion: The closure executed when done. Passes if the task should be retried or not if failed.
    @discardableResult
    func startBatchUpload(writeKey: String, data: Data, completion: @escaping (_ result: Result<Bool, Error>) -> Void) -> (any UploadTask)? {
        guard let uploadURL = segmentURL(for: apiHost, path: "/b") else {
            self.analytics?.reportInternalError(HTTPClientErrors.failedToOpenBatch)
            completion(.failure(HTTPClientErrors.failedToOpenBatch))
            return nil
        }
          
        let urlRequest = configuredRequest(for: uploadURL, method: "POST")
        
        let dataTask = session.uploadTask(with: urlRequest, from: data) { [weak self] (data, response, error) in
            guard let self else { return }
            handleResponse(data: data, response: response, error: error, url: uploadURL, completion: completion)
        }
        
        dataTask.resume()
        return dataTask
    }
    
    private func handleResponse(data: Data?, response: URLResponse?, error: Error?, url: URL?, batch: URL? = nil, completion: @escaping (_ result: Result<Bool, Error>) -> Void) {
        var apiResponse = ""
        
        if let data {
            apiResponse = String(decoding: data, as: UTF8.self)
        }
        if let error = error {
            analytics?.log(message: "Error uploading request \(error.localizedDescription).")
            analytics?.reportInternalError(AnalyticsError.networkUnknown(apiResponse, error))
            completion(.failure(HTTPClientErrors.unknown(error: error)))
        } else if let httpResponse = response as? HTTPURLResponse {
            switch (httpResponse.statusCode) {
            case 1..<300:
                completion(.success(true))
                return
            case 300..<400:
                analytics?.reportInternalError(AnalyticsError.networkUnexpectedHTTPCode(apiResponse, httpResponse.statusCode))
                completion(.failure(HTTPClientErrors.statusCode(code: httpResponse.statusCode)))
            case 429:
                analytics?.reportInternalError(AnalyticsError.networkServerLimited(apiResponse, httpResponse.statusCode))
                completion(.failure(HTTPClientErrors.statusCode(code: httpResponse.statusCode)))
                
            case 400:
                do {
                    if let batchUrl = batch {
                        let fileData = try Data(contentsOf: batchUrl)
                        let fileString = String(describing: String(data: fileData, encoding: .utf8))
                        analytics?.reportInternalError(AnalyticsError.networkUnexpectedHTTPCode(apiResponse + fileString, httpResponse.statusCode))
                    } else {
                        analytics?.reportInternalError(AnalyticsError.networkUnexpectedHTTPCode(apiResponse, httpResponse.statusCode))
                    }
                } catch {
                    analytics?.reportInternalError(AnalyticsError.networkUnexpectedHTTPCode("Empty File " + apiResponse, httpResponse.statusCode))
                }
            default:
                analytics?.reportInternalError(AnalyticsError.networkServerRejected(apiResponse, httpResponse.statusCode))
                completion(.failure(HTTPClientErrors.statusCode(code: httpResponse.statusCode)))
            }
        }
    }
    
    func settingsFor(writeKey: String, completion: @escaping (Bool, Settings?) -> Void) {
        guard let settingsURL = segmentURL(for: cdnHost, path: "/projects/\(writeKey)/settings") else {
            completion(false, nil)
            return
        }

        let urlRequest = configuredRequest(for: settingsURL, method: "GET")

        let dataTask = session.dataTask(with: urlRequest) { [weak self] (data, response, error) in
            if let error = error {
                self?.analytics?.reportInternalError(AnalyticsError.settingsFail(AnalyticsError.networkUnknown("", error)))
                completion(false, nil)
                return
            }

            if let httpResponse = response as? HTTPURLResponse {
                if httpResponse.statusCode > 300 {
                    self?.analytics?.reportInternalError(AnalyticsError.settingsFail(AnalyticsError.networkUnexpectedHTTPCode("", httpResponse.statusCode)))
                    completion(false, nil)
                    return
                }
            }

            guard let data = data else {
                self?.analytics?.reportInternalError(AnalyticsError.settingsFail(AnalyticsError.networkInvalidData))
                completion(false, nil)
                return
            }

            do {
                let responseJSON = try JSONDecoder.default.decode(Settings.self, from: data)
                completion(true, responseJSON)
            } catch {
                self?.analytics?.reportInternalError(AnalyticsError.settingsFail(AnalyticsError.jsonUnableToDeserialize(error)))
                completion(false, nil)
                return
            }

        }

        dataTask.resume()
    }

    deinit {
        // finish any tasks that may be processing
        session.finishTasksAndInvalidate()
    }
}


extension HTTPClient {
    static func authorizationHeaderForWriteKey(_ key: String) -> String {
        var returnHeader: String = ""
        let rawHeader = "\(key):"
        if let encodedRawHeader = rawHeader.data(using: .utf8) {
            returnHeader = encodedRawHeader.base64EncodedString(options: NSData.Base64EncodingOptions.init(rawValue: 0))
        }
        return returnHeader
    }

    internal static func getDefaultAPIHost() -> String {
        return Self.defaultAPIHost
    }

    internal static func getDefaultCDNHost() -> String {
        return Self.defaultCDNHost
    }

    internal func configuredRequest(for url: URL, method: String) -> URLRequest {
        var request = URLRequest(url: url, cachePolicy: .reloadIgnoringLocalCacheData, timeoutInterval: 60)
        request.httpMethod = method
        request.addValue("application/json; charset=utf-8", forHTTPHeaderField: "Content-Type")
        request.addValue("analytics-ios/\(Analytics.version())", forHTTPHeaderField: "User-Agent")
        request.addValue("gzip", forHTTPHeaderField: "Accept-Encoding")

        if let requestFactory = analytics?.configuration.values.requestFactory {
            request = requestFactory(request)
        }

        return request
    }
}
extension URLRequest {
    /// The textual representation used when written to an output stream, in the form of a cURL command.
    func cURLRepresentation() -> String {
        var components = ["$ curl -v"]
        
        let request = self
        guard let url = request.url else {
            return "$ curl command could not be created"
        }
        
        if let httpMethod = request.httpMethod, httpMethod != "GET" {
            components.append("-X \(httpMethod)")
        }
        
        var headers: [AnyHashable: Any] = [:]
        
        request.allHTTPHeaderFields?.forEach { headers[$0.0] = $0.1 }//.filter { $0.0 != "Cookie" }
        
        components += headers.map {
            let escapedValue = String(describing: $0.value).replacingOccurrences(of: "\"", with: "\\\"")
            
            return "-H \"\($0.key): \(escapedValue)\""
        }
        
        if let httpBodyData = request.httpBody, let httpBody = String(data: httpBodyData, encoding: .utf8) {
            var escapedBody = httpBody.replacingOccurrences(of: "\\\"", with: "\\\\\"")
            escapedBody = escapedBody.replacingOccurrences(of: "\"", with: "\\\"")
            
            components.append("-d \"\(escapedBody)\"")
        }
        
        components.append("\"\(url.absoluteString)\"")
        
        return components.joined(separator: " \\\n\t")
    }
}
