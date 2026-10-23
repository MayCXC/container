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
import Foundation

extension Application {
    /// The stdio side of a remote builder connection, the way buildctl's
    /// dial-stdio is for buildkitd: a peer runs this over ssh and speaks to
    /// the builder through it, so a remote machine needs no listener beyond
    /// sshd and no verbs beyond the ones already here.
    /// https://github.com/moby/buildkit/blob/master/client/connhelper/ssh/ssh.go
    public struct BuilderDialStdio: AsyncLoggableCommand {
        public static var configuration: CommandConfiguration {
            var config = CommandConfiguration()
            config.commandName = "dial-stdio"
            config.abstract = "Proxy the standard streams to the builder"
            return config
        }

        @Option(name: .long, help: ArgumentHelp("Builder shim vsock port", valueName: "port"))
        var vsockPort: UInt32 = 8088

        @OptionGroup
        public var logOptions: Flags.Logging

        public init() {}

        public func run() async throws {
            let client = ContainerClient()
            let builder = try await client.dial(id: "buildkit", port: vsockPort)

            // Each direction relays what its side holds as it arrives, the way
            // ProcessIO relays a process's output: availableData returns
            // whatever a pipe or socket has and is empty at end of file,
            // where a read for a count waits for that count or end of file,
            // which a conversation of small messages never reaches. The proxy
            // is over when either side stops, since a half-closed
            // conversation with a builder has nothing left to say.
            let stdin = FileHandle.standardInput
            let stdout = FileHandle.standardOutput
            let (stopped, stop) = AsyncStream<Void>.makeStream()
            for (source, sink) in [(stdin, builder), (builder, stdout)] {
                source.readabilityHandler = { handle in
                    let data = handle.availableData
                    if data.isEmpty {
                        handle.readabilityHandler = nil
                        stop.yield()
                        return
                    }
                    do {
                        try sink.write(contentsOf: data)
                    } catch {
                        handle.readabilityHandler = nil
                        stop.yield()
                    }
                }
            }

            for await _ in stopped {
                break
            }
            stdin.readabilityHandler = nil
            builder.readabilityHandler = nil
            try? builder.close()
        }
    }
}
