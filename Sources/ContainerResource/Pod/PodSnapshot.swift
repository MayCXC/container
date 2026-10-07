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

import ContainerizationOCI
import Foundation

/// A snapshot of a pod along with its configuration
/// and any runtime state information.
///
/// The shape follows the runtime interface's `PodSandboxStatus`.
/// https://github.com/kubernetes/cri-api/blob/master/pkg/apis/runtime/v1/api.proto
public struct PodSnapshot: Codable, Sendable {
    /// The configuration of the pod.
    public var configuration: PodConfiguration

    /// Identifier of the pod.
    public var id: String {
        configuration.id
    }

    /// Configured platform for the pod.
    public var platform: ContainerizationOCI.Platform {
        configuration.platform
    }

    /// The runtime state of the pod.
    public var state: PodState

    /// Network interfaces attached to the pod.
    public var networks: [Attachment]

    /// Identifiers of the containers in the pod.
    public var containers: [String]

    /// When the pod was started.
    public var startedDate: Date?

    /// The disk images the pod's machine holds, by host path: each volume
    /// one of its containers brought, held from when the machine attached it
    /// until the machine stops, whether or not a container still mounts it.
    /// Empty when the machine is not running.
    public var diskImages: [String]

    public init(
        configuration: PodConfiguration,
        state: PodState,
        networks: [Attachment],
        containers: [String] = [],
        startedDate: Date? = nil,
        diskImages: [String] = []
    ) {
        self.configuration = configuration
        self.state = state
        self.networks = networks
        self.containers = containers
        self.startedDate = startedDate
        self.diskImages = diskImages
    }

    /// What holds the disk image at `path` among `pods`. A pod someone named
    /// is named itself, since its machine holds a volume until the pod stops
    /// whichever of its containers brought it; a pod a container was given is
    /// named by its containers, since it stops with them. Kubernetes names the
    /// pods using a volume another node is refused, and the pods a claim's
    /// deletion waits on.
    /// https://github.com/kubernetes/kubernetes/blob/master/pkg/controller/volume/attachdetach/reconciler/reconciler.go
    /// https://kubernetes.io/docs/concepts/storage/persistent-volumes/#storage-object-in-use-protection
    public static func holders(ofImage path: String, in pods: [PodSnapshot]) -> (containers: [String], pods: [String]) {
        var containers: [String] = []
        var named: [String] = []
        for pod in pods where pod.diskImages.contains(path) {
            if pod.configuration.isAnonymous {
                containers += pod.containers
            } else {
                named.append(pod.id)
            }
        }
        return (containers.sorted(), named.sorted())
    }
}
