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

import ContainerResource

/// Where a sandbox's machine is in its life.
///
/// A sandbox has a state of its own, apart from the containers in it: the
/// runtime interface reports a sandbox ready or not ready and each container
/// created, running or exited, and a sandbox is ready from the moment it is
/// brought up, holding no container yet. The phases here are the machine's,
/// and `ready` is the one in which it takes and runs containers.
/// https://github.com/kubernetes/cri-api/blob/master/pkg/apis/runtime/v1/api.proto
public enum SandboxStatus: String, Codable, Sendable {
    /// The service is up and the machine is waiting to be booted.
    case created
    /// The machine is booted and takes containers.
    case ready
    /// The machine is on its way down.
    case stopping
    /// The machine has stopped.
    case stopped
    /// The service is exiting; nothing boots again under it.
    case shuttingDown
}

/// A snapshot of a sandbox and its resources.
public struct SandboxSnapshot: Codable, Sendable {
    /// Where the sandbox's machine is in its life.
    public var status: SandboxStatus
    /// Network attachments for the sandbox.
    public var networks: [Attachment]
    /// Containers placed in the sandbox, each with its own status.
    public var containers: [ContainerSnapshot]

    public init(
        status: SandboxStatus,
        networks: [Attachment],
        containers: [ContainerSnapshot]
    ) {
        self.status = status
        self.networks = networks
        self.containers = containers
    }
}
