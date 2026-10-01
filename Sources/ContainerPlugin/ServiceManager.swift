//===----------------------------------------------------------------------===//
// Copyright © 2025-2026 Apple Inc. and the container project authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//===----------------------------------------------------------------------===//

import ContainerizationError
import Foundation

public struct ServiceManager {
    private static func runLaunchctlCommand(args: [String]) throws -> Int32 {
        let launchctl = Foundation.Process()
        launchctl.executableURL = URL(fileURLWithPath: "/bin/launchctl")
        launchctl.arguments = args

        let null = FileHandle.nullDevice
        launchctl.standardOutput = null
        launchctl.standardError = null

        try launchctl.run()
        launchctl.waitUntilExit()

        return launchctl.terminationStatus
    }

    /// Register a service by providing the path to a plist.
    public static func register(plistPath: String) throws {
        let domain = try Self.getDomainString()
        _ = try runLaunchctlCommand(args: ["bootstrap", domain, plistPath])
    }

    /// Deregister a service by a launchd label.
    public static func deregister(fullServiceLabel label: String) throws {
        _ = try runLaunchctlCommand(args: ["bootout", label])
    }

    /// Deregister a service and pass return status
    public static func deregister(fullServiceLabel label: String, status: inout Int32) throws {
        status = try runLaunchctlCommand(args: ["bootout", label])
    }

    /// Deregister a service and wait until launchd has let go of the job.
    ///
    /// `bootout` returns while the job is still being torn down: for some
    /// tens of milliseconds after it, `launchctl list` no longer names the
    /// label while `launchctl print` still describes the job, signalled and
    /// not yet gone, holding its Mach service port. A definition registered
    /// under the label meanwhile is taken, but a connection made to the label
    /// before the old job's port is torn down reaches that port and is
    /// interrupted when it goes, and the new job never spawns. launchctl
    /// offers nothing to wait on for the teardown, so the job is asked for
    /// until `print` no longer finds it, up to the timeout.
    public static func deregisterAndWait(fullServiceLabel label: String, timeout: Duration = .seconds(30)) async throws {
        var status: Int32 = 0
        try Self.deregister(fullServiceLabel: label, status: &status)
        let deadline = ContinuousClock.now + timeout
        while try Self.jobExists(fullServiceLabel: label) {
            guard ContinuousClock.now < deadline else {
                throw ContainerizationError(
                    .internalError,
                    message: "launchd still holds the job \(label) \(timeout) after `launchctl bootout` returned \(status)")
            }
            try await Task.sleep(for: .milliseconds(100))
        }
    }

    /// Whether launchd still holds a job object for the label, in any state.
    /// `print` describes the job until launchd has torn it down, where `list`
    /// names only one that runs or waits to run.
    private static func jobExists(fullServiceLabel label: String) throws -> Bool {
        try runLaunchctlCommand(args: ["print", label]) == 0
    }

    /// Restart a service by a launchd label.
    public static func kickstart(fullServiceLabel label: String) throws {
        _ = try runLaunchctlCommand(args: ["kickstart", "-k", label])
    }

    /// Send a signal to a service by a launchd label.
    public static func kill(fullServiceLabel label: String, signal: Int32 = 15) throws {
        _ = try runLaunchctlCommand(args: ["kill", "\(signal)", label])
    }

    /// Retrieve labels for all loaded launch units.
    public static func enumerate() throws -> [String] {
        let launchctl = Foundation.Process()
        launchctl.executableURL = URL(fileURLWithPath: "/bin/launchctl")
        launchctl.arguments = ["list"]

        let stdoutPipe = Pipe()
        let stderrPipe = Pipe()
        launchctl.standardOutput = stdoutPipe
        launchctl.standardError = stderrPipe

        try launchctl.run()
        let outputData = stdoutPipe.fileHandleForReading.readDataToEndOfFile()
        let stderrData = stderrPipe.fileHandleForReading.readDataToEndOfFile()
        launchctl.waitUntilExit()
        let status = launchctl.terminationStatus
        guard status == 0 else {
            throw ContainerizationError(
                .internalError, message: "command `launchctl list` failed with status \(status), message: \(String(data: stderrData, encoding: .utf8) ?? "no error message")")
        }

        guard let outputText = String(data: outputData, encoding: .utf8) else {
            throw ContainerizationError(
                .internalError, message: "could not decode output of command `launchctl list`, message: \(String(data: stderrData, encoding: .utf8) ?? "no error message")")
        }

        // The third field of each line of launchctl list output is the label
        return outputText.split { $0.isNewline }
            .map { String($0).split { $0.isWhitespace } }
            .filter { $0.count >= 3 }
            .map { String($0[2]) }
    }

    /// Check if a service has been registered or not.
    public static func isRegistered(fullServiceLabel label: String) throws -> Bool {
        let exitStatus = try runLaunchctlCommand(args: ["list", label])
        return exitStatus == 0
    }

    private static func getLaunchdSessionType() throws -> String {
        let launchctl = Foundation.Process()
        launchctl.executableURL = URL(fileURLWithPath: "/bin/launchctl")
        launchctl.arguments = ["managername"]

        let null = FileHandle.nullDevice
        let stdoutPipe = Pipe()
        launchctl.standardOutput = stdoutPipe
        launchctl.standardError = null

        try launchctl.run()
        let outputData = stdoutPipe.fileHandleForReading.readDataToEndOfFile()
        launchctl.waitUntilExit()
        let status = launchctl.terminationStatus
        guard status == 0 else {
            throw ContainerizationError(.internalError, message: "command `launchctl managername` failed with status \(status)")
        }
        guard let outputText = String(data: outputData, encoding: .utf8) else {
            throw ContainerizationError(.internalError, message: "could not decode output of command `launchctl managername`")
        }
        return outputText.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    public static func getDomainString() throws -> String {
        let currentSessionType = try getLaunchdSessionType()
        switch currentSessionType {
        case LaunchPlist.Domain.System.rawValue:
            return LaunchPlist.Domain.System.rawValue.lowercased()
        case LaunchPlist.Domain.Background.rawValue:
            return "user/\(getuid())"
        case LaunchPlist.Domain.Aqua.rawValue:
            return "gui/\(getuid())"
        default:
            throw ContainerizationError(.internalError, message: "unsupported session type \(currentSessionType)")
        }
    }
}
