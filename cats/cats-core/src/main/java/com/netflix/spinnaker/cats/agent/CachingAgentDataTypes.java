/*
 * Copyright 2019 Google, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.cats.agent;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import java.util.Collection;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;

/**
 * This is the list of data types that are returned by a given {@link CachingAgent}.
 *
 * <p>All keys that a CachingAgent puts into in {@link CacheResult#getCacheResults()} should be
 * listed in this class.
 *
 * <p>Authoritative types are those for which the caching agent knows everything. The {@link
 * CacheData} for these types should have attributes and relationships. If a object of authoritative
 * type was returned in a previous caching run but not in the current caching run, it will be
 * assumed to have been deleted, and will be removed from the cache.
 *
 * <p>Informative types are those for which the caching agent knows about their relationships to the
 * authoritative types, but doesn't know more than that. These should not contain attributes, only
 * relationships. The caching executor treats these as additive and will not delete relationships
 * from the cache just because they aren't returned here.
 *
 * <p>The On-Demand type is used for loading data in the middle of the cycle. It can contain
 * attributes but not relationships.
 */
@EqualsAndHashCode
public final class CachingAgentDataTypes {

  private final ImmutableSet<String> authoritativeTypes;
  private final ImmutableSet<String> informativeTypes;
  private final String onDemandType;

  private CachingAgentDataTypes(Builder builder) {
    this.authoritativeTypes = builder.authoritativeTypes;
    this.informativeTypes = builder.informativeTypes;
    this.onDemandType = builder.onDemandType;
  }

  public ImmutableSet<String> getAuthoritativeTypes() {
    return authoritativeTypes;
  }

  public ImmutableSet<String> getInformativeTypes() {
    return informativeTypes;
  }

  public Optional<String> getOnDemandType() {
    return Optional.ofNullable(onDemandType);
  }

  /** Returns authoritative, informative, and (if present) the on-demand type. */
  public ImmutableSet<String> getAllDeclaredTypes() {
    ImmutableSet.Builder<String> result =
        ImmutableSet.<String>builder()
            .addAll(getAuthoritativeTypes())
            .addAll(getInformativeTypes());
    getOnDemandType().ifPresent(result::add);
    return result.build();
  }

  public static Builder builder() {
    return new Builder().authoritativeTypes(ImmutableSet.of()).informativeTypes(ImmutableSet.of());
  }

  public static final class Builder {

    private ImmutableSet<String> authoritativeTypes;
    private ImmutableSet<String> informativeTypes;
    private String onDemandType;

    public Builder authoritativeTypes(Collection<String> authoritativeTypes) {
      this.authoritativeTypes = ImmutableSet.copyOf(authoritativeTypes);
      return this;
    }

    public Builder authoritativeTypes(String... authoritativeTypes) {
      return authoritativeTypes(ImmutableSet.copyOf(authoritativeTypes));
    }

    public Builder informativeTypes(Collection<String> informativeTypes) {
      this.informativeTypes = ImmutableSet.copyOf(informativeTypes);
      return this;
    }

    public Builder informativeTypes(String... informativeTypes) {
      return informativeTypes(ImmutableSet.copyOf(informativeTypes));
    }

    public Builder onDemandType(@Nullable String onDemandType) {
      this.onDemandType = onDemandType;
      return this;
    }

    public CachingAgentDataTypes build() {
      validate();
      return new CachingAgentDataTypes(this);
    }

    private void validate() {
      checkArgument(
          !authoritativeTypes.isEmpty() || !informativeTypes.isEmpty(),
          "Must add either authoritative or informative types (or both)");
      SetView<String> overlappingTypes = Sets.intersection(authoritativeTypes, informativeTypes);
      checkArgument(
          overlappingTypes.isEmpty(),
          "The following types are listed as both authoritative and informative: %s",
          overlappingTypes);
      if (onDemandType != null) {
        checkArgument(
            !authoritativeTypes.contains(onDemandType),
            "On-Demand type '%s' is also listed as authoritative",
            onDemandType);
        checkArgument(
            !informativeTypes.contains(onDemandType),
            "On-Demand type '%s' is also listed as informative",
            onDemandType);
      }
    }
  }
}
