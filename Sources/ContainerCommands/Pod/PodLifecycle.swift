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

import ArgumentParser
import ContainerAPIClient
import ContainerResource
import ContainerizationError
import ContainerizationExtras
import Foundation

/// Act on several pods at once and report every failure, the way the container
/// verbs act on several containers: each pod is attempted whatever became of
/// the others, each that succeeded is printed, and the failures are thrown
/// together.
private func actOnPods(_ names: [String], _ act: @Sendable @escaping (String) async throws -> Void) async throws {
    var errors: [any Error] = []
    await withTaskGroup(of: (any Error)?.self) { group in
        for name in names {
            group.addTask {
                do {
                    try await act(name)
                    print(name)
                    return nil
                } catch {
                    return error
                }
            }
        }

        for await error in group {
            if let error {
                errors.append(error)
            }
        }
    }

    if !errors.isEmpty {
        throw AggregateError(errors)
    }
}

extension Application.PodCommand {
    public struct PodStart: AsyncLoggableCommand {
        public static let configuration = CommandConfiguration(
            commandName: "start",
            abstract: "Boot a pod's machine and start the containers in it"
        )

        @OptionGroup
        public var logOptions: Flags.Logging

        @Argument(help: "Pods to start")
        var names: [String]

        public init() {}

        public func run() async throws {
            // The caller's agent rides into every member the machine boots,
            // the donation each sibling boot path carries.
            var dynamicEnv: [String: String] = [:]
            if let agent = ProcessInfo.processInfo.environment["SSH_AUTH_SOCK"] {
                dynamicEnv["SSH_AUTH_SOCK"] = agent
            }
            let env = dynamicEnv
            try await actOnPods(names) { try await ClientPod.start($0, dynamicEnv: env) }
        }
    }

    public struct PodStop: AsyncLoggableCommand {
        public static let configuration = CommandConfiguration(
            commandName: "stop",
            abstract: "Stop a pod's machine, and with it every container inside"
        )

        @OptionGroup
        public var logOptions: Flags.Logging

        @Argument(help: "Pods to stop")
        var names: [String]

        public init() {}

        public func run() async throws {
            try await actOnPods(names) { try await ClientPod.stop($0) }
        }
    }

    public struct PodDelete: AsyncLoggableCommand {
        public static let configuration = CommandConfiguration(
            commandName: "delete",
            abstract: "Delete one or more pods",
            aliases: ["rm"]
        )

        @OptionGroup
        public var logOptions: Flags.Logging

        @Flag(name: .shortAndLong, help: "Delete the pod's containers along with it")
        var force: Bool = false

        @Argument(help: "Pods to delete")
        var names: [String]

        public init() {}

        public func run() async throws {
            let force = self.force
            try await actOnPods(names) { try await ClientPod.delete($0, force: force) }
        }
    }

    public struct PodUpdate: AsyncLoggableCommand {
        public static let configuration = CommandConfiguration(
            commandName: "update",
            abstract: "Hold a running pod to a memory size, which its containers share"
        )

        @OptionGroup
        public var logOptions: Flags.Logging

        @Option(
            name: .shortAndLong,
            help: """
                Memory the pod's machine is to hold (1MiByte granularity), with optional K, M, G, \
                T, or P suffix. The guest gives back the difference, and takes it again when the \
                size is raised.
                """
        )
        var memory: String

        @Argument(help: "Pod to hold")
        var name: String

        public init() {}

        public func run() async throws {
            let bytes = try Parser.memoryStringAsMiB(memory).mib()
            try await ClientPod.update(name, memoryInBytes: bytes)
            print(name)
        }
    }

    public struct PodInspect: AsyncLoggableCommand {
        public static let configuration = CommandConfiguration(
            commandName: "inspect",
            abstract: "Display information about one or more pods"
        )

        @OptionGroup
        public var logOptions: Flags.Logging

        @Argument(help: "Pods to inspect")
        var names: [String]

        public init() {}

        public func run() async throws {
            var snapshots: [PodSnapshot] = []
            for name in Set(names).sorted() {
                snapshots.append(try await ClientPod.inspect(name))
            }
            try Output.emit(Output.renderJSON(snapshots, options: .pretty))
        }
    }

    public struct PodList: AsyncLoggableCommand {
        public static let configuration = CommandConfiguration(
            commandName: "list",
            abstract: "List pods",
            aliases: ["ls"]
        )

        @OptionGroup
        public var logOptions: Flags.Logging

        @Flag(name: .shortAndLong, help: "Only output the pod names")
        var quiet: Bool = false

        @Option(name: .long, help: "Format of the output")
        var format: ListFormat = .table

        public init() {}

        public func run() async throws {
            let pods = try await ClientPod.list()
            try Output.render(payload: pods, display: pods, format: format, quiet: quiet)
        }
    }
}
