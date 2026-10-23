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
import Testing

@testable import ContainerBuild

struct BuildExporterTests {
    private func transfer(id: String, metadata: [String: String]) -> ServerStream {
        var transfer = BuildTransfer()
        transfer.id = id
        transfer.direction = .outof
        transfer.complete = true
        transfer.metadata = metadata
        var packet = ServerStream()
        packet.buildID = id
        packet.buildTransfer = transfer
        return packet
    }

    @Test func testAcceptsOnlyTheExporterStage() throws {
        let exporter = BuildExporter()
        #expect(try exporter.accept(transfer(id: "a", metadata: ["stage": "exporter", "method": "Respond"])))
        #expect(try !exporter.accept(transfer(id: "b", metadata: ["stage": "fssync"])))
    }

    @Test func testRespondRecordsTheResponseAndAcknowledgesIt() async throws {
        let exporter = BuildExporter()
        let (replies, sender) = AsyncStream<ClientStream>.makeStream()
        let digest = "sha256:417ad241ef42ecdd12b03e5508aaa6c2a42c93bc4c042695f1c93b81b565068b"
        try await exporter.handle(
            sender,
            transfer(
                id: "transfer-1",
                metadata: [
                    "os": "linux",
                    "stage": "exporter",
                    "method": "Respond",
                    "containerimage.digest": digest,
                ]))
        sender.finish()

        #expect(await exporter.response == ["containerimage.digest": digest])
        var acknowledgments: [ClientStream] = []
        for await reply in replies {
            acknowledgments.append(reply)
        }
        #expect(acknowledgments.count == 1)
        let acknowledgment = try #require(acknowledgments.first)
        #expect(acknowledgment.buildID == "transfer-1")
        guard case .buildTransfer(let reply) = acknowledgment.packetType else {
            Issue.record("the acknowledgment is a BuildTransfer")
            return
        }
        #expect(reply.id == "transfer-1")
        #expect(reply.metadata["stage"] == "exporter")
    }

    @Test func testAnUnknownMethodFails() async throws {
        let exporter = BuildExporter()
        let (_, sender) = AsyncStream<ClientStream>.makeStream()
        await #expect(throws: BuildExporter.Error.self) {
            try await exporter.handle(sender, transfer(id: "c", metadata: ["stage": "exporter", "method": "Write"]))
        }
        #expect(await exporter.response.isEmpty)
    }
}
