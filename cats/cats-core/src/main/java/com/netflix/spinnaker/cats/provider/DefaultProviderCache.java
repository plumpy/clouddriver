/*
 * Copyright 2014 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.cats.provider;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.netflix.spinnaker.cats.agent.CacheResult;
import com.netflix.spinnaker.cats.agent.CachingAgentDataTypes;
import com.netflix.spinnaker.cats.cache.CacheData;
import com.netflix.spinnaker.cats.cache.CacheFilter;
import com.netflix.spinnaker.cats.cache.DefaultCacheData;
import com.netflix.spinnaker.cats.cache.RelationshipCacheFilter;
import com.netflix.spinnaker.cats.cache.WriteableCache;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of ProviderCache that writes through to a provided backing WriteableCache.
 *
 * <p>This implementation will handle aggregating results from multiple sources, and the view
 * methods will merge relationships from all sources into a single relationship.
 */
public class DefaultProviderCache implements ProviderCache {

  private static final String ALL_ID = "_ALL_"; // dirty = true
  private static final Map<String, Object> ALL_ATTRIBUTE =
      Collections.unmodifiableMap(
          new HashMap<String, Object>(1) {
            {
              put("id", ALL_ID);
            }
          });

  private final WriteableCache backingStore;

  public DefaultProviderCache(WriteableCache backingStore) {
    this.backingStore = backingStore;
  }

  @Override
  public CacheData get(String type, String id) {
    return get(type, id, null);
  }

  @Override
  public CacheData get(String type, String id, CacheFilter cacheFilter) {
    validateTypes(type);
    if (ALL_ID.equals(id)) {
      return null;
    }
    CacheData item = backingStore.get(type, id, cacheFilter);
    if (item == null) {
      return null;
    }

    return mergeRelationships(item);
  }

  @Override
  public Collection<CacheData> getAll(String type) {
    return getAll(type, (CacheFilter) null);
  }

  @Override
  public Collection<CacheData> getAll(String type, CacheFilter cacheFilter) {
    validateTypes(type);
    Collection<CacheData> all = backingStore.getAll(type, cacheFilter);
    return buildResponse(all);
  }

  @Override
  public Collection<CacheData> getAll(String type, Collection<String> identifiers) {
    return getAll(type, identifiers, null);
  }

  @Override
  public Collection<CacheData> getAll(
      String type, Collection<String> identifiers, CacheFilter cacheFilter) {
    validateTypes(type);
    Collection<CacheData> byId = backingStore.getAll(type, identifiers, cacheFilter);
    return buildResponse(byId);
  }

  @Override
  public Collection<CacheData> getAll(String type, String... identifiers) {
    return getAll(type, Arrays.asList(identifiers));
  }

  @Override
  public Collection<String> existingIdentifiers(String type, Collection<String> identifiers) {
    Set<String> existing = new HashSet<>(backingStore.existingIdentifiers(type, identifiers));
    existing.remove(ALL_ID);
    return existing;
  }

  @Override
  public Collection<String> getIdentifiers(String type) {
    validateTypes(type);
    Set<String> identifiers = new HashSet<>(backingStore.getIdentifiers(type));
    identifiers.remove(ALL_ID);
    return identifiers;
  }

  @Override
  public Collection<String> filterIdentifiers(String type, String glob) {
    validateTypes(type);
    Set<String> identifiers = new HashSet<>(backingStore.filterIdentifiers(type, glob));
    identifiers.remove(ALL_ID);

    return identifiers;
  }

  @Override
  public void putCacheResult(
      String sourceAgentType, CachingAgentDataTypes agentDataTypes, CacheResult cacheResult) {

    SetMultimap<String, String> evictionsByType = HashMultimap.create();

    // We add all the evictions first, then remove those for which data has been provided in the
    // CacheResult.
    cacheResult.getEvictions().forEach(evictionsByType::putAll);
    for (String type : agentDataTypes.getAuthoritativeTypes()) {
      Set<String> existingIds = getExistingSourceIdentifiers(type, sourceAgentType);
      Set<String> incomingIds =
          cacheResult.getCacheResults().getOrDefault(type, ImmutableSet.of()).stream()
              .map(CacheData::getId)
              .collect(toImmutableSet());
      SetView<String> missingIds = Sets.difference(existingIds, incomingIds);
      evictionsByType.putAll(type, missingIds);
    }

    for (String type : agentDataTypes.getAllDeclaredTypes()) {
      Collection<CacheData> cacheData =
          cacheResult.getCacheResults().getOrDefault(type, ImmutableList.of());
      cacheDataType(type, sourceAgentType, cacheData);
      cacheData.forEach(data -> evictionsByType.remove(type, data.getId()));
    }

    // The only thing actually in here should be the on-demand type. Once CachingAgentDataTypes is
    // properly populated across all caching providers, we can remove this, at which point data
    // returned in CacheResults that isn't in CacheResultDataTypes will be ignored.
    SetView<String> undeclaredTypes =
        Sets.difference(
            cacheResult.getCacheResults().keySet(), agentDataTypes.getAllDeclaredTypes());
    for (String type : undeclaredTypes) {
      Collection<CacheData> cacheData =
          cacheResult.getCacheResults().getOrDefault(type, ImmutableList.of());
      cacheDataType(type, sourceAgentType, cacheData);
      cacheData.forEach(data -> evictionsByType.remove(type, data.getId()));
    }

    for (Map.Entry<String, Collection<String>> eviction : evictionsByType.asMap().entrySet()) {
      evictDeletedItems(eviction.getKey(), eviction.getValue());
    }
  }

  @Override
  public void addCacheResult(
      String sourceAgentType, CachingAgentDataTypes agentDataTypes, CacheResult cacheResult) {
    Set<String> allTypes = new HashSet<>(cacheResult.getCacheResults().keySet());
    validateTypes(allTypes);

    allTypes.forEach(
        type -> cacheDataType(type, sourceAgentType, cacheResult.getCacheResults().get(type)));
  }

  @Override
  public void putCacheData(String sourceAgentType, CacheData cacheData) {
    backingStore.merge(sourceAgentType, cacheData);
  }

  private void validateTypes(String... types) {
    validateTypes(Arrays.asList(types));
  }

  private void validateTypes(Collection<String> types) {
    Set<String> invalid = new HashSet<>();
    for (String type : types) {
      if (!validType(type)) {
        invalid.add(type);
      }
    }
    if (!invalid.isEmpty()) {
      throw new IllegalArgumentException("Types contain unsupported characters: " + invalid);
    }
  }

  private boolean validType(String type) {
    return type.indexOf(':') == -1;
  }

  private Collection<CacheData> buildResponse(Collection<CacheData> source) {
    Collection<CacheData> response = new ArrayList<>(source.size());
    for (CacheData item : source) {
      if (!ALL_ID.equals(item.getId())) {
        response.add(mergeRelationships(item));
      }
    }
    return Collections.unmodifiableCollection(response);
  }

  private Set<String> getExistingSourceIdentifiers(String type, String sourceAgentType) {
    CacheData all =
        backingStore.get(type, ALL_ID, RelationshipCacheFilter.include(sourceAgentType));
    if (all == null) {
      return new HashSet<>();
    }
    return ImmutableSet.copyOf(
        all.getRelationships().getOrDefault(sourceAgentType, ImmutableSet.of()));
  }

  private void cacheDataType(String type, String sourceAgentType, Collection<CacheData> items) {
    Collection<String> idSet = new HashSet<>();

    int ttlSeconds = -1;
    Collection<CacheData> toStore = new ArrayList<>(items.size() + 1);
    for (CacheData item : items) {
      idSet.add(item.getId());
      toStore.add(uniqueifyRelationships(item, sourceAgentType));

      if (item.getTtlSeconds() > ttlSeconds) {
        ttlSeconds = item.getTtlSeconds();
      }
    }
    Map<String, Collection<String>> allRelationship = new HashMap<>();
    allRelationship.put(sourceAgentType, idSet);

    toStore.add(new DefaultCacheData(ALL_ID, ttlSeconds, ALL_ATTRIBUTE, allRelationship));
    backingStore.mergeAll(type, toStore);
  }

  private CacheData uniqueifyRelationships(CacheData source, String sourceAgentType) {
    Map<String, Collection<String>> relationships = new HashMap<>(source.getRelationships().size());
    for (Map.Entry<String, Collection<String>> entry : source.getRelationships().entrySet()) {
      relationships.put(entry.getKey() + ':' + sourceAgentType, entry.getValue());
    }
    return new DefaultCacheData(
        source.getId(), source.getTtlSeconds(), source.getAttributes(), relationships);
  }

  private CacheData mergeRelationships(CacheData source) {
    Map<String, Collection<String>> relationships = new HashMap<>(source.getRelationships().size());
    for (Map.Entry<String, Collection<String>> entry : source.getRelationships().entrySet()) {
      int idx = entry.getKey().indexOf(':');
      if (idx == -1) {
        throw new IllegalStateException("Expected delimiter in relationship key");
      }
      String type = entry.getKey().substring(0, idx);
      Collection<String> values = relationships.get(type);
      if (values == null) {
        values = new HashSet<>();
        relationships.put(type, values);
      }
      values.addAll(entry.getValue());
    }
    return new DefaultCacheData(source.getId(), source.getAttributes(), relationships);
  }

  @Override
  public void evictDeletedItems(String type, Collection<String> ids) {
    backingStore.evictAll(type, ids);
  }
}
