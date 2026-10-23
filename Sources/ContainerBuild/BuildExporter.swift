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

import Foundation

/// Handles the `exporter` stage of the build protocol.
///
/// Once the solve returns, the builder hands back the solve's exporter
/// response, the map a BuildKit client receives in its solve response, over
/// the build's own stream. A store export names the image it built there by
/// its root digest (`containerimage.digest`), so the image is recorded
/// wherever the builder runs.
/// https://github.com/moby/buildkit/blob/master/exporter/containerimage/exptypes/keys.go
actor BuildExporter: BuildPipelineHandler {
    /// The exporter response the builder reported, empty until it arrives.
    private(set) var response: [String: String] = [:]

    /// The entries that route the packet rather than belong to the response.
    private static let routing: Set<String> = ["os", "stage", "method"]

    nonisolated func accept(_ packet: ServerStream) throws -> Bool {
        guard let buildTransfer = packet.getBuildTransfer() else {
            return false
        }
        return buildTransfer.stage() == "exporter"
    }

    func handle(_ sender: AsyncStream<ClientStream>.Continuation, _ packet: ServerStream) async throws {
        guard let buildTransfer = packet.getBuildTransfer() else {
            throw Error.buildTransferMissing
        }
        guard buildTransfer.method() == "Respond" else {
            throw Error.unknownMethod(buildTransfer.method() ?? "")
        }
        self.response = buildTransfer.metadata.filter { !Self.routing.contains($0.key) }

        // The builder lets the build end only once this arrives, so the
        // response is never lost to the stream closing.
        var transfer = BuildTransfer()
        transfer.id = buildTransfer.id
        transfer.direction = .outof
        transfer.metadata = [
            "os": "linux",
            "stage": "exporter",
        ]
        var reply = ClientStream()
        reply.buildID = packet.buildID
        reply.buildTransfer = transfer
        reply.packetType = .buildTransfer(transfer)
        sender.yield(reply)
    }
}

extension BuildExporter {
    enum Error: Swift.Error, CustomStringConvertible {
        case buildTransferMissing
        case unknownMethod(String)

        var description: String {
            switch self {
            case .buildTransferMissing:
                return "buildTransfer field missing in packet"
            case .unknownMethod(let method):
                return "exporter method \(method) is unknown, expected 'Respond'"
            }
        }
    }
}
