/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "isolator.hpp"
#include "process_isolator.hpp"
#ifdef __linux__
#include "cgroups_isolator.hpp"
#endif


namespace mesos {
namespace internal {
namespace slave {

Isolator* Isolator::create(const std::string &type)
{
  if (type == "process") {
    return new ProcessIsolator();
#ifdef __linux__
  } else if (type == "cgroups") {
    return new CgroupsIsolator();
#endif
  }

  return NULL;
}


void Isolator::destroy(Isolator* isolator)
{
  if (isolator != NULL) {
    delete isolator;
  }
}

// TODO(benh): Move this computation into Flags as the "default".
// TODO(vinod): Move some of this computation into Resources.
process::Future<Resources> Isolator::getResources(const Flags& flags)
{

  Try<Resources> parse = Resources::parse(
      flags.resources.isSome() ? flags.resources.get() : "",
      flags.default_role);
  CHECK_SOME(parse);
  Resources resources = parse.get();

  if (!resources.cpus().isSome()) {
    double cpus;

    Try<long> cpus_ = os::cpus();
    if (!cpus_.isSome()) {
      LOG(WARNING) << "Failed to auto-detect the number of cpus to use: '"
                   << cpus_.error()
                   << "' ; defaulting to " << DEFAULT_CPUS;
      cpus = DEFAULT_CPUS;
    } else {
      cpus = cpus_.get();
    }

    Resource r = Resources::parse(
        "cpus",
        stringify(cpus),
        flags.default_role).get();
    resources += r;
  }


  if (!resources.mem().isSome()) {
    Bytes mem;

    Try<Bytes> mem_ = os::memory();
    if (!mem_.isSome()) {
      LOG(WARNING) << "Failed to auto-detect the size of main memory: '"
                   << mem_.error()
                   << "' ; defaulting to " << DEFAULT_MEM;
      mem = DEFAULT_MEM;
    } else {
      mem = mem_.get();

      // Leave 1 GB free if we have more than 1 GB, otherwise, use all!
      // TODO(benh): Have better default scheme (e.g., % of mem not
      // greater than 1 GB?)
      if (mem > Gigabytes(1)) {
        mem = mem - Gigabytes(1);
      }
    }

    Resource r = Resources::parse(
        "mem",
        stringify(mem.megabytes()),
        flags.default_role).get();
    resources += r;
  }

  if (!resources.disk().isSome()) {
    Bytes disk;

    // NOTE: We calculate disk size of the file system on
    // which the slave work directory is mounted.
    Try<Bytes> disk_ = fs::size(flags.work_dir);
    if (!disk_.isSome()) {
      LOG(WARNING) << "Failed to auto-detect the disk space: '"
                   << disk_.error()
                   << "' ; defaulting to " << DEFAULT_DISK;
      disk = DEFAULT_DISK;
    } else {
      disk = disk_.get();

      // Leave 5 GB free if we have more than 10 GB, otherwise, use all!
      // TODO(benh): Have better default scheme (e.g., % of disk not
      // greater than 10 GB?)
      if (disk > Gigabytes(10)) {
        disk = disk - Gigabytes(5);
      }
    }

    Resource r = Resources::parse(
        "disk",
        stringify(disk.megabytes()),
        flags.default_role).get();
    resources += r;
  }

  if (!resources.ports().isSome()) {
    Resource r = Resources::parse(
        "ports",
        stringify(DEFAULT_PORTS),
        flags.default_role).get();
    resources += r;
  }
  return resources;
}
} // namespace slave {
} // namespace internal {
} // namespace mesos {
