//===----------------------------------------------------------------------===//
// Copyright © 2026 Apple Inc. and the container project authors.
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

import ContainerPlugin
import ContainerTestSupport
import Foundation
import Testing

/// A containers service starting over containers that are still running, and
/// over containers it did not hear go. Every test restarts the service each
/// other test is talking to, so the suite runs alone.
@Suite(.serialized)
struct TestCLIContainersServiceRestartSerial {
    /// The launchd service the containers service runs in.
    private static let containersService = "com.apple.container.apiserver"

    /// A container still running when the containers service restarts is
    /// adopted by the one that comes up, one run to be removed on exit
    /// included, and its exit afterwards removes it with the pod it was given.
    @Test func testRunningAutoRemoveContainerIsAdopted() async throws {
        try await ContainerFixture.with { f in
            try await f.withContainer(image: WarmupImage.alpine320.rawValue) { name in
                let pod = try f.inspectContainer(name).configuration.pod

                try ServiceManager.kickstart(fullServiceLabel: try Self.target(Self.containersService))
                try await Self.waitUntilServing(f)

                #expect(try f.getContainerStatus(name) == "running")
                try f.doStop(name)
                try await Self.waitUntilGone(f, ["inspect", name])
                try await Self.waitUntilGone(f, ["pod", "inspect", pod])
            }
        }
    }

    /// A container run to be removed on exit whose machine went while no
    /// containers service was there to hear it is removed by the next one to
    /// start, with the pod it was given.
    @Test func testAutoRemoveContainerGoneUnheardIsRemoved() async throws {
        try await ContainerFixture.with { f in
            try await f.withContainer(image: WarmupImage.alpine320.rawValue) { name in
                let configuration = try f.inspectContainer(name).configuration
                let service = try Self.target(Self.containersService)
                let machine = try Self.target(
                    "com.apple.container.\(configuration.runtimeHandler).\(configuration.pod)")

                // Held still, the service cannot hear the machine go.
                try ServiceManager.kill(fullServiceLabel: service, signal: SIGSTOP)
                try ServiceManager.kill(fullServiceLabel: machine, signal: SIGKILL)
                try ServiceManager.kill(fullServiceLabel: service, signal: SIGKILL)
                try ServiceManager.kickstart(fullServiceLabel: service)
                try await Self.waitUntilServing(f)

                try await Self.waitUntilGone(f, ["inspect", name])
                try await Self.waitUntilGone(f, ["pod", "inspect", configuration.pod])
            }
        }
    }

    /// The launchd target for a label, in whichever of this user's domains
    /// holds it: a system started from a terminal no window server owns is
    /// registered in `user/<uid>`, and one started from a login session in
    /// `gui/<uid>` (ServiceManager.deregisterAnyDomain).
    private static func target(_ label: String) throws -> String {
        for domain in try ServiceManager.getDomainStrings() {
            let print = Process()
            print.executableURL = URL(fileURLWithPath: "/bin/launchctl")
            print.arguments = ["print", "\(domain)/\(label)"]
            print.standardOutput = FileHandle.nullDevice
            print.standardError = FileHandle.nullDevice
            try print.run()
            print.waitUntilExit()
            if print.terminationStatus == 0 {
                return "\(domain)/\(label)"
            }
        }
        throw CommandError.executionFailed("no launchd domain of this user's holds \(label)")
    }

    /// The service answers once it has adopted what runs and removed what is
    /// left to remove, since it listens only after both.
    private static func waitUntilServing(_ f: ContainerFixture) async throws {
        for _ in 0..<60 {
            if (try? f.run(["list"]))?.status == 0 {
                return
            }
            try await Task.sleep(for: .seconds(1))
        }
        throw CommandError.executionFailed("the containers service did not come back")
    }

    private static func waitUntilGone(_ f: ContainerFixture, _ arguments: [String]) async throws {
        for _ in 0..<60 {
            if let result = try? f.run(arguments), result.status != 0 {
                return
            }
            try await Task.sleep(for: .seconds(1))
        }
        throw CommandError.executionFailed("`container \(arguments.joined(separator: " "))` still answers")
    }
}
