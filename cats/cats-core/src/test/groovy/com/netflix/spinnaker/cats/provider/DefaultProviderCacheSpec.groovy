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

package com.netflix.spinnaker.cats.provider

import com.netflix.spinnaker.cats.agent.CachingAgentDataTypes
import com.netflix.spinnaker.cats.agent.DefaultCacheResult
import com.netflix.spinnaker.cats.cache.DefaultCacheData
import com.netflix.spinnaker.cats.mem.InMemoryCache

class DefaultProviderCacheSpec extends ProviderCacheSpec<DefaultProviderCache> {

  @Override
  DefaultProviderCache getSubject() {
    backingStore = new InMemoryCache()
    new DefaultProviderCache(backingStore)
  }

  def 'authoritative types not present in cache result are removed from the backing store'() {
    setup:
    String agent = 'agent'
    def cachingAgentTypes = CachingAgentDataTypes.builder().authoritativeTypes('type').build()
    cache.putCacheResult(agent, cachingAgentTypes,
      new DefaultCacheResult(['type': [new DefaultCacheData('id', ['attribute': 'value'], /* relationships= */ [:])]]))

    when:
    cache.putCacheResult(agent, cachingAgentTypes, new DefaultCacheResult([:]))

    then:
    cache.get('type', 'id') == null
    backingStore.get('type', 'id') == null
    !backingStore.get('type', '_ALL_')?.getRelationships()?.get(agent)
  }

  def 'informative types not present in cache result remain in the backing store'() {
    setup:
    String agent = 'agent'
    def cachingAgentTypes = CachingAgentDataTypes.builder().informativeTypes('type').build()
    cache.putCacheResult(agent, cachingAgentTypes,
      new DefaultCacheResult(['type': [new DefaultCacheData('parent', ['attribute':'value'], /* relationships= */ ['type':{'child'}])]]))
    cache.putCacheResult(agent, cachingAgentTypes,
      new DefaultCacheResult(['type': [new DefaultCacheData('child', [:], /* relationships= */ [:])]]))

    when:
    cache.putCacheResult(agent, cachingAgentTypes, new DefaultCacheResult([:]))

    then:
    backingStore.get('type', '_ALL_').getRelationships().get(agent).contains('child')
  }
}
