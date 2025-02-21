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
    private static let defaultAPIHost = ""//"api.segment.io/v1"
    private static let defaultCDNHost = ""//"cdn-settings.segment.com/v1"

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
        guard let uploadURL = segmentURL(for: apiHost, path: "") else {
            self.analytics?.reportInternalError(HTTPClientErrors.failedToOpenBatch)
            completion(.failure(HTTPClientErrors.failedToOpenBatch))
            return nil
        }

        
        do {
            let fileData = try Data(contentsOf: batch)
            if let fileString = String(data: fileData, encoding: .utf8) {
                print("File contents before upload:\n\(fileString)")
            } else {
                print("Unable to convert file data to a string")
            }
        } catch {
            print("Error reading file: \(error)")
        }
        
        
        
        let urlRequest = configuredRequest(for: uploadURL, method: "POST")
        


        let dataTask = session.uploadTask(with: urlRequest, fromFile: batch) { [weak self] (data, response, error) in
            guard let self else { return }
            
            print("\nAPI Request as curl \n \(urlRequest.cURLRepresentation())")

            handleResponse(data: data, response: response, error: error, url: uploadURL, completion: completion)
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
    
    private func handleResponse(data: Data?, response: URLResponse?, error: Error?, url: URL?, completion: @escaping (_ result: Result<Bool, Error>) -> Void) {
        if let error = error {
            analytics?.log(message: "Error uploading request \(error.localizedDescription).")
            analytics?.reportInternalError(AnalyticsError.networkUnknown(url, error))
            completion(.failure(HTTPClientErrors.unknown(error: error)))
        } else if let httpResponse = response as? HTTPURLResponse {
            switch (httpResponse.statusCode) {
            case 1..<300:
                if let response = getResponseFromData(data: data).responseData {
                    print(response)
                }
                
                completion(.success(true))
                return
            case 300..<400:
                analytics?.reportInternalError(AnalyticsError.networkUnexpectedHTTPCode(url, httpResponse.statusCode))
                completion(.failure(HTTPClientErrors.statusCode(code: httpResponse.statusCode)))
            case 429:
                analytics?.reportInternalError(AnalyticsError.networkServerLimited(url, httpResponse.statusCode))
                completion(.failure(HTTPClientErrors.statusCode(code: httpResponse.statusCode)))
            default:
                analytics?.reportInternalError(AnalyticsError.networkServerRejected(url, httpResponse.statusCode))
                completion(.failure(HTTPClientErrors.statusCode(code: httpResponse.statusCode)))
            }
        }
    }
    
    func getResponseFromData(data: Data?) -> (responseData: [AnyHashable : Any]?, error: Error?) {
        guard let data = data else { return (nil, nil) }
        do {
            let responseJSON = try JSONSerialization.jsonObject(with: data, options: .allowFragments)
            if let responseData = responseJSON as? [AnyHashable : Any] {
                return (responseData, nil)
            } else if let responseData = responseJSON as? [Any] {
                return (["array" : responseData], nil)
            } else {
                return (nil, NSError(domain: "Segment", code: 0, userInfo: [NSLocalizedDescriptionKey : "unable to parse server response"]))
            }
        } catch let error {
            return (nil, error)
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
                self?.analytics?.reportInternalError(AnalyticsError.settingsFail(AnalyticsError.networkUnknown(settingsURL, error)))
                completion(false, nil)
                return
            }

            if let httpResponse = response as? HTTPURLResponse {
                if httpResponse.statusCode > 300 {
                    self?.analytics?.reportInternalError(AnalyticsError.settingsFail(AnalyticsError.networkUnexpectedHTTPCode(settingsURL, httpResponse.statusCode)))
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
