/*
 * Copyright 2019 MapD Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef QUERYENGINE_TABLEPARTITIONERRUNTIME_H
#define QUERYENGINE_TABLEPARTITIONERRUNTIME_H

#include <cstdint>
#include "../Shared/funcannotations.h"

extern "C" ALWAYS_INLINE DEVICE uint32_t MurmurHash32(const void* key, int len);
extern "C" ALWAYS_INLINE DEVICE uint64_t MurmurHash64(const void* key, int len);

// These versions are used for single component keys to reduce
// module compilation time.
extern "C" ALWAYS_INLINE DEVICE uint32_t MurmurHash32_4(uint32_t key);
extern "C" ALWAYS_INLINE DEVICE uint32_t MurmurHash32_8(uint64_t key);
extern "C" ALWAYS_INLINE DEVICE uint64_t MurmurHash64_4(uint32_t key);
extern "C" ALWAYS_INLINE DEVICE uint64_t MurmurHash64_8(uint64_t key);

#endif  // QUERYENGINE_TABLEPARTITIONERRUNTIME_H