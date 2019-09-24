/*
 * Copyright 2017 MapD Technologies, Inc.
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

#ifndef QUERYENGINE_MURMURHASH_H
#define QUERYENGINE_MURMURHASH_H

#include <cstdint>
#include <type_traits>
#include "../Shared/funcannotations.h"

extern "C" NEVER_INLINE DEVICE uint32_t MurmurHash1(const void* key,
                                                    int len,
                                                    const uint32_t seed);

extern "C" NEVER_INLINE DEVICE uint64_t MurmurHash64A(const void* key,
                                                      int len,
                                                      uint64_t seed);

template <typename T>
FORCE_INLINE DEVICE typename std::make_unsigned<T>::type MurmurHash(const void* key,
                                                                    int len,
                                                                    typename std::make_unsigned<T>::type seed);

template <>
FORCE_INLINE DEVICE uint32_t MurmurHash<int32_t>(const void* key,
                                                 int len,
                                                 uint32_t seed) {
  return MurmurHash1(key, len, seed);
}

template <>
FORCE_INLINE DEVICE uint64_t MurmurHash<int64_t>(const void* key,
                                                 int len,
                                                 uint64_t seed) {
  return MurmurHash64A(key, len, seed);
}

#endif  // QUERYENGINE_MURMURHASH_H
