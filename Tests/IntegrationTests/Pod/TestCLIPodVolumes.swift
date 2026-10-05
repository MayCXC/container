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

/// A volume is held by the machines whose containers mount it, and another
/// machine refused the volume is told which containers hold it.
@Suite
struct TestCLIPodVolumes {
    private let alpine = WarmupImage.alpine320

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
}
