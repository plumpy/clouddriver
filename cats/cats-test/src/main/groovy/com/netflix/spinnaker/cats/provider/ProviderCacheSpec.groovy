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

import com.netflix.spinnaker.cats.agent.CacheResult
import com.netflix.spinnaker.cats.agent.CachingAgentDataTypes
import com.netflix.spinnaker.cats.agent.DefaultCacheResult
import com.netflix.spinnaker.cats.cache.CacheData
import com.netflix.spinnaker.cats.cache.CacheSpec
import com.netflix.spinnaker.cats.cache.DefaultCacheData
import com.netflix.spinnaker.cats.cache.WriteableCache

abstract class ProviderCacheSpec<T extends ProviderCache> extends CacheSpec<T> {

  WriteableCache backingStore

  void populateOne(String type, String id, CacheData data) {
    cache.putCacheResult('testAgent', authoritativeTypes(), new DefaultCacheResult((type): [data]))
  }

  def 'explicit evictions are removed from the cache'() {
    setup:
    String agent = 'agent'
    CacheResult result = new DefaultCacheResult(test: [new DefaultCacheData('id', [id: 'id'], [:])])
    cache.putCacheResult(agent, authoritativeTypes(), result)

    when:
    def data = cache.get('test', 'id')

    then:
    data != null
    data.id == 'id'

    when:
    cache.putCacheResult(agent, authoritativeTypes(), new DefaultCacheResult([:], [test: ['id']]))
    data = cache.get('test', 'id')

    then:
    data == null
  }

  def 'multiple agents can cache the same data type'() {
    setup:
    String usEast1Agent = 'AwsProvider:test/us-east-1/ClusterCachingAgent'
    CacheResult testUsEast1 = buildCacheResult('test', 'us-east-1')
    String usWest2Agent = 'AwsProvider:test/us-west-2/ClusterCachingAgent'
    CacheResult testUsWest2 = buildCacheResult('test', 'us-west-2')
    cache.putCacheResult(usEast1Agent, authoritativeTypes('serverGroup', 'cluster', 'application'), testUsEast1)
    cache.putCacheResult(usWest2Agent, authoritativeTypes('serverGroup', 'cluster', 'application'), testUsWest2)

    when:
    def app = cache.get('application', 'testapp')

    then:
    app.attributes.accountName == 'test'
    app.relationships.serverGroup.sort() == ['test/us-east-1/testapp-test-v001', 'test/us-west-2/testapp-test-v001']
  }

  def "an agents deletions don't affect another agent"() {
    setup:
    String usEast1Agent = 'AwsProvider:test/us-east-1/ClusterCachingAgent'
    CacheResult testUsEast1 = buildCacheResult('test', 'us-east-1')
    String usWest2Agent = 'AwsProvider:test/us-west-2/ClusterCachingAgent'
    CacheResult testUsWest2 = buildCacheResult('test', 'us-west-2')
    cache.putCacheResult(usEast1Agent, authoritativeTypes('serverGroup', 'cluster', 'application'), testUsEast1)
    cache.putCacheResult(usWest2Agent, authoritativeTypes('serverGroup', 'cluster', 'application'), testUsWest2)

    when:
    def app = cache.get('application', 'testapp')

    then:
    app.attributes.accountName == 'test'
    app.relationships.serverGroup.sort() == ['test/us-east-1/testapp-test-v001', 'test/us-west-2/testapp-test-v001']

    when:
    testUsEast1 = buildCacheResult('test', 'us-east-1', 'v002')
    cache.putCacheResult(usEast1Agent, authoritativeTypes('serverGroup', 'cluster', 'application'), testUsEast1)
    app = cache.get('application', 'testapp')

    then:
    app.relationships.serverGroup.sort() == ['test/us-east-1/testapp-test-v002', 'test/us-west-2/testapp-test-v001']

  }

  def "items can be evicted by type and id"() {
    setup:
    String usEast1Agent = 'AwsProvider:test/us-east-1/ClusterCachingAgent'
    CacheResult testUsEast1 = buildCacheResult('test', 'us-east-1')
    cache.putCacheResult(usEast1Agent, authoritativeTypes('serverGroup'), testUsEast1)

    when:
    def sg = cache.get('serverGroup', 'test/us-east-1/testapp-test-v001')

    then:
    sg != null

    when:
    cache.evictDeletedItems('serverGroup', ['test/us-east-1/testapp-test-v001'])
    sg = cache.get('serverGroup', 'test/us-east-1/testapp-test-v001')

    then:
    sg == null
  }

  CachingAgentDataTypes authoritativeTypes(String... types) {
    if (types) {
      CachingAgentDataTypes.builder().authoritativeTypes(types).build()
    } else {
      // CachingAgentDataTypes requires at least one type
      CachingAgentDataTypes.builder().informativeTypes("noSuchType").build()
    }
  }

  private CacheResult buildCacheResult(String account, String region, String sgVersion = 'v001') {
    String serverGroup = "$account/$region/testapp-test-$sgVersion"
    String cluster = "$account/testapp-test"
    String application = 'testapp'
    String loadbalancer = "$account/$region/testapp--frontend"
    Map<String, Object> serverGroupAtts = [
      name   : 'testapp-test-v001',
      account: account,
      region : region
    ]

    CacheData app = new DefaultCacheData(application, [accountName: account], [serverGroup: [serverGroup], cluster: [cluster]])
    CacheData sg = new DefaultCacheData(serverGroup, serverGroupAtts, [application: [application], cluster: [cluster], loadBalancer: [loadbalancer]])
    CacheData clu = new DefaultCacheData(cluster, [:], [application: [application], serverGroup: [serverGroup]])
    CacheData lb = new DefaultCacheData(loadbalancer, [:], [serverGroup: [serverGroup]])

    new DefaultCacheResult([application: [app], serverGroup: [sg], cluster: [clu], loadBalancer: [lb]])
  }
}
