//
//   Copyright 2018  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

namespace java io.warp10.continuum.thrift.data

/**
 * Generic logging event
 */
struct LoggingEvent {
  /**
   * Map of attributes
   */
  1: map<string,string> attributes,
}

/**
 * Structure to serialize the parameters from an HyperLogLogPlus instance
 */
struct HyperLogLogPlusParameters {
  1: required i64 initTime,
  2: required byte p,
  3: required byte pprime,
  4: required bool sparse,
  5: optional i32 sparseListLen,
  6: optional binary sparseList,
  7: optional binary registers,
  8: optional bool gzipped,
  /**
   * Key these HLL parameters relate to (producer, app, ...)
   */
  9: optional string key,
}

/**
 * Structure used by ScriptRunner's scheduler to publish in Kafka
 * script to be run by workers.
 */
 
struct RunRequest {
  /**
   * Timestamp at which the script was scheduled
   */
  1: i64 scheduledAt,
  /**
   * Periodicity of the script
   */
  2: i64 periodicity,
  /**
   * Path to the script
   */
  3: string path,
  /**
   * Content of the script
   */
  4: binary content,
  /**
   * Flag indicating whether the content is compressed or not
   */
  5: bool compressed,
  /**
   * Id of the scheduling node
   */
  6: string scheduler,
}
