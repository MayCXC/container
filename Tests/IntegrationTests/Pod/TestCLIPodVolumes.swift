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

import ContainerTestSupport
import Foundation
import Testing

/// A volume is held by the machine that attached it, a pod's until the pod
/// stops, and another machine refused the volume, or a deletion of it, is told
/// what holds it.
@Suite
struct TestCLIPodVolumes {
    private let alpine = WarmupImage.alpine320

    private struct Held: Decodable {
        let diskImages: [String]
    }

    private struct Volume: Decodable {
        struct Configuration: Decodable {
            let source: String
        }
        let configuration: Configuration
    }

    @Test func testVolumeRefusalNamesHolder() async throws {
        try await ContainerFixture.with { f in
            let vol = "\(f.testID)-vol"
            let holder = "\(f.testID)-holder"
            let refused = "\(f.testID)-refused"
            f.addCleanup {
                try? f.doRemoveIfExists(holder, force: true, ignoreFailure: true)
                try? f.doRemoveIfExists(refused, force: true, ignoreFailure: true)
                f.doVolumeDeleteIfExists(vol)
            }

            try f.doVolumeCreate(vol)
            try await f.doLongRun(name: holder, image: alpine.rawValue, args: ["-v", "\(vol):/data"], autoRemove: false, waitUntilRunning: true)

            let result = try f.run(["run", "--name", refused, "-v", "\(vol):/data", alpine.rawValue, "true"])
            #expect(result.status != 0, "a second machine was given a volume another writes")
            #expect(result.error.contains(holder), "the refusal should name the container holding \(vol): \(result.error)")
        }
    }

    /// The member that brought a volume leaves the running pod, and the pod
    /// keeps the volume until it stops: its inspect lists the volume's image,
    /// another machine is refused it and its deletion is refused, both naming
    /// the pod, and a new member is given it with what the first one wrote.
    /// Once the pod stops another machine reads the write.
    @Test func testPodHoldsVolumeUntilItStops() async throws {
        try await ContainerFixture.with { f in
            let pod = "\(f.testID)-pod"
            let vol = "\(f.testID)-vol"
            let seed = "\(f.testID)-seed"
            let writer = "\(f.testID)-writer"
            let joiner = "\(f.testID)-joiner"
            let refused = "\(f.testID)-refused"
            f.addCleanup {
                for name in [seed, writer, joiner, refused] {
                    try? f.doRemoveIfExists(name, force: true, ignoreFailure: true)
                }
                _ = try? f.run(["pod", "delete", "--force", pod])
                f.doVolumeDeleteIfExists(vol)
            }

            try f.doVolumeCreate(vol)
            let image = try JSONDecoder().decode([Volume].self, from: Data(try f.run(["volume", "inspect", vol]).check().output.utf8))
            try f.run(["pod", "create", pod]).check()
            try f.run(["create", "--name", seed, "--pod", pod, alpine.rawValue, "sleep", "infinity"]).check()
            try f.run(["create", "--name", writer, "--pod", pod, "-v", "\(vol):/data", alpine.rawValue, "sleep", "infinity"]).check()
            try f.run(["pod", "start", pod]).check()
            try f.doExec(writer, cmd: ["sh", "-c", "echo kept > /data/kept"])
            try f.doStop(writer)
            try f.doRemove(writer)

            let inspect = try JSONDecoder().decode([Held].self, from: Data(try f.run(["pod", "inspect", pod]).check().output.utf8))
            #expect(inspect.first?.diskImages == image.map(\.configuration.source), "\(pod) should hold \(vol)'s image with its member gone: \(inspect)")

            let other = try f.run(["run", "--name", refused, "-v", "\(vol):/data", alpine.rawValue, "true"])
            #expect(other.status != 0, "another machine was given a volume the running pod holds")
            #expect(other.error.contains(pod), "the refusal should name \(pod): \(other.error)")

            let delete = try f.run(["volume", "delete", vol])
            #expect(delete.status != 0, "the volume the running pod holds was deleted")
            #expect(delete.error.contains(pod), "the delete refusal should name \(pod): \(delete.error)")

            try f.run(["create", "--name", joiner, "--pod", pod, "-v", "\(vol):/data", alpine.rawValue, "sleep", "infinity"]).check()
            let join = try f.run(["start", joiner])
            #expect(join.status == 0, "a new member of \(pod) was refused the pod's volume: \(join.error)")
            let kept = try f.doExec(joiner, cmd: ["cat", "/data/kept"])
            #expect(kept.trimmingCharacters(in: .whitespacesAndNewlines) == "kept", "the new member read \(kept)")

            try f.run(["pod", "stop", pod]).check()
            let after = try f.run(["run", "--rm", "-v", "\(vol):/data", alpine.rawValue, "cat", "/data/kept"])
            #expect(after.status == 0, "another machine was refused the volume once \(pod) stopped: \(after.error)")
            #expect(after.output.trimmingCharacters(in: .whitespacesAndNewlines) == "kept", "another machine read \(after.output)")
        }
    }
}
