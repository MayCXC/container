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

/// A container joining a named pod runs in a machine that was booted before it
/// existed, so the flags that describe the machine are the pod's and a
/// container asking for one is refused by the flag's name.
@Suite
struct TestCLIPodJoin {
    private let alpine = WarmupImage.alpine320

    /// The flags that belong to the pod's machine, each with a value a container
    /// could plausibly pass. A refusal names the flag; nothing is fetched or
    /// booted for a container that is refused.
    private static let held: [(flag: String, value: String?)] = [
        ("--kernel", "/nonexistent/vmlinux"),
        ("--kernel-arg", "quiet"),
        ("--init-image", "ghcr.io/nonexistent/vminit:none"),
        ("--rosetta", nil),
        ("--virtualization", nil),
    ]

    @Test func testJoiningPodRefusesMachineFlags() async throws {
        try await ContainerFixture.with { f in
            let pod = "\(f.testID)-pod"
            try f.run(["pod", "create", pod]).check()
            f.addCleanup { _ = try? f.run(["pod", "delete", "--force", pod]) }

            for (flag, value) in Self.held {
                var args = ["run", "--rm", "--pod", pod, flag]
                if let value { args.append(value) }
                args += [alpine.rawValue, "true"]
                let result = try f.run(args)
                #expect(result.status != 0, "\(flag) should be refused for a container joining \(pod)")
                #expect(result.error.contains(flag), "the refusal should name \(flag): \(result.error)")
                #expect(
                    result.error.contains("not the container's to ask for"),
                    "the refusal should say whose the flag is: \(result.error)")
            }
        }
    }

    @Test func testJoiningPodWithoutMachineFlagsRuns() async throws {
        try await ContainerFixture.with { f in
            let pod = "\(f.testID)-pod"
            try f.run(["pod", "create", pod]).check()
            f.addCleanup { _ = try? f.run(["pod", "delete", "--force", pod]) }

            let result = try f.run(["run", "--rm", "--pod", pod, alpine.rawValue, "echo", "joined"])
            #expect(result.status == 0, "a container asking for nothing of the machine's joins: \(result.error)")
            #expect(result.output.trimmingCharacters(in: .whitespacesAndNewlines) == "joined")
        }
    }
}
