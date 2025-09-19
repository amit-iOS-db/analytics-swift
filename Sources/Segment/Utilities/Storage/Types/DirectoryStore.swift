//
//  File.swift
//  
//
//  Created by Brandon Sneed on 3/2/24.
//

import Foundation

public class DirectoryStore: DataStore {
    internal static var fileValidator: ((URL) -> Void)? = nil
    
    public typealias StoreConfiguration = Configuration
    
    private let batchHeader = "{ \"batch\": ["
    private let batchHeaderWithNL = "{ \"batch\": [\n"  // writeLine() adds '\n' anyway
    
    public struct Configuration {
        let writeKey: String
        let storageLocation: URL
        let baseFilename: String
        let maxFileSize: Int
        let indexKey: String
        
        public init(writeKey: String, storageLocation: URL, baseFilename: String, maxFileSize: Int, indexKey: String) {
            self.writeKey = writeKey
            self.storageLocation = storageLocation
            self.baseFilename = baseFilename
            self.maxFileSize = maxFileSize
            self.indexKey = indexKey
        }
    }
    
    public var hasData: Bool {
        return count > 0
    }
    
    public var count: Int {
        if let r = try? FileManager.default.contentsOfDirectory(at: config.storageLocation, includingPropertiesForKeys: nil) {
            return r.count
        }
        return 0
    }
    
    public var transactionType: DataTransactionType {
        return .file
    }
    
    static let tempExtension = "temp"
    internal let config: Configuration
    internal var writer: LineStreamWriter? = nil
    internal let userDefaults: UserDefaults
    
    public required init(configuration: Configuration) {
        try? FileManager.default.createDirectory(at: configuration.storageLocation, withIntermediateDirectories: true)
        self.config = configuration
        self.userDefaults = UserDefaults(suiteName: "com.segment.storage.\(config.writeKey)")!
    }
    
    public func reset() {
        let files = sortedFiles(includeUnfinished: true)
        remove(data: files)
    }
    
    public func append(data: RawEvent) {
        
        _ = startFileIfNeeded()
        guard let writer else { return }

        // Size cap
        if writer.bytesWritten >= config.maxFileSize {
            finishFile()
            append(data: data)  // write into a fresh file
            return
        }

        let line = data.toString()
        do {
            let comma = needsCommaBeforeNextItem(writer.url) ? "," : ""
            try writer.writeLine(comma + line)
        } catch {
        }
    }
    
    private func needsCommaBeforeNextItem(_ url: URL) -> Bool {
        guard let fh = try? FileHandle(forReadingFrom: url) else { return false }
        defer { fh.closeFile() }

        let tailWindow = 256
        let size = (try? FileManager.default.attributesOfItem(atPath: url.path)[.size] as? NSNumber)?.intValue ?? 0
        let seek = max(0, size - tailWindow)
        try? fh.seek(toOffset: UInt64(seek))

        let data = fh.readDataToEndOfFile()

        // Scan backwards for first non-whitespace
        for byte in data.reversed() {
            switch byte {
            case 0x20, 0x09, 0x0A, 0x0D: // space, \t, \n, \r
                continue
            case 0x5B: // '[' → header’s opening bracket: first item
                return false
            default:
                return true   // already had something → needs comma
            }
        }
        return false // treat as brand new
    }
    
    public func fetch(count: Int?, maxBytes: Int?) -> DataResult? {
        if writer != nil {
            finishFile()
        }
        let sorted = sortedFiles()
        var data = sorted
        
        if let maxBytes {
            data = upToSize(max: UInt64(maxBytes), files: data)
        }
        
        if let count, count <= data.count {
            data = Array(data[0..<count])
        }
        
        if data.count > 0 {
            return DataResult(dataFiles: data, removable: data)
        }
        return nil
    }
    
    public func remove(data: [DataStore.ItemID]) {
        guard let urls = data as? [URL] else { return }
        for file in urls {
            try? FileManager.default.removeItem(at: file)
        }
    }
}

extension DirectoryStore {
    func sortedFiles(includeUnfinished: Bool = false) -> [URL] {
        guard let allFiles = try? FileManager.default.contentsOfDirectory(at: config.storageLocation, includingPropertiesForKeys: nil) else {
            return []
        }
        let files = allFiles.filter { file in
            if includeUnfinished {
                return true
            }
            return file.pathExtension == Self.tempExtension
        }
        let sorted = files.sorted { left, right in
            return left.lastPathComponent < right.lastPathComponent
        }
        return sorted
    }
    
    func upToSize(max: UInt64, files: [URL]) -> [URL] {
        var result = [URL]()
        var accumulatedSize: UInt64 = 0
        
        for file in files {
            if let attrs = try? FileManager.default.attributesOfItem(atPath: file.path) {
                guard let s = attrs[FileAttributeKey.size] as? Int else { continue }
                let size = UInt64(s)
                if accumulatedSize + size < max {
                    result.append(file)
                    accumulatedSize += size
                }
            }
        }
        return result
    }
    
    @inline(__always)
    func startFileIfNeeded() -> Bool {
       
        guard writer == nil else { return false }

        let index = getIndex()
        let fileURL = config.storageLocation.appendingPathComponent("\(index)-\(config.baseFilename)")
        writer = LineStreamWriter(url: fileURL)
        guard let writer else { return false }

        var wroteHeader = false

        if writer.bytesWritten == 0 {
            // brand new/empty file → start fresh
            try? writer.fileHandle.truncate(atOffset: 0)
            writer.bytesWritten = 0
            try? writer.writeLine(batchHeader)
            wroteHeader = true
        } else {
            // Reopened file: ensure it begins with a valid header
            if !fileBeginsWithBatchHeader(url: fileURL) {
                try? writer.fileHandle.truncate(atOffset: 0)
                writer.bytesWritten = 0
                try? writer.writeLine(batchHeader)
                wroteHeader = true
            }
        }

        return wroteHeader
    }
    
    private func fileBeginsWithBatchHeader(url: URL) -> Bool {
        guard let fh = try? FileHandle(forReadingFrom: url) else { return false }
        defer { fh.closeFile() }

        let want = batchHeaderWithNL.utf8.count
        let head = fh.readData(ofLength: want)
        let s = String(decoding: head, as: UTF8.self)
        return s.hasPrefix(batchHeader)
    }
    
    func finishFile() {
        
        guard let writer else {
            #if DEBUG
            assertionFailure("There's no working file!")
            #endif
            return
        }

        let sentAt = Date().iso8601()
        let fileEnding = "],\"sentAt\":\"\(sentAt)\",\"writeKey\":\"\(config.writeKey)\"}"
        try? writer.writeLine(fileEnding)

        // Flush & normalize size (no sparse padding)
        if #available(iOS 13.0, *) {
            _ = try? writer.fileHandle.synchronize()
        } else {
            writer.fileHandle.synchronizeFile()
        }
        if let attrs = try? FileManager.default.attributesOfItem(atPath: writer.url.path),
           let size = attrs[.size] as? NSNumber {
            try? writer.fileHandle.truncate(atOffset: size.uint64Value)
        }

        let url = writer.url
        DirectoryStore.fileValidator?(url)

        let newURL = url.appendingPathExtension(Self.tempExtension)
        try? FileManager.default.moveItem(at: url, to: newURL)
        self.writer = nil
        incrementIndex()
    }
}

extension DirectoryStore {
    func getIndex() -> Int {
        let index: Int = userDefaults.integer(forKey: config.indexKey)
        return index
    }
    
    func incrementIndex() {
        let index: Int = userDefaults.integer(forKey: config.indexKey) + 1
        userDefaults.set(index, forKey: config.indexKey)
        userDefaults.synchronize()
    }
}
