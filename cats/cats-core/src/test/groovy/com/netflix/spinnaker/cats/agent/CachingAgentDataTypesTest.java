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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

@RunWith(JUnitPlatform.class)
final class CachingAgentDataTypesTest {

  @Test
  void getAllDeclaredTypes_withOnDemand() {
    CachingAgentDataTypes dataTypes =
        CachingAgentDataTypes.builder()
            .authoritativeTypes("authoritative1", "authoritative2")
            .informativeTypes("informative1", "informative2")
            .onDemandType("onDemand")
            .build();

    assertThat(dataTypes.getAllDeclaredTypes())
        .containsExactly(
            "authoritative1", "authoritative2", "informative1", "informative2", "onDemand");
  }

  @Test
  void getAllDeclaredTypes_withoutOnDemand() {
    CachingAgentDataTypes dataTypes =
        CachingAgentDataTypes.builder()
            .authoritativeTypes("authoritative1", "authoritative2")
            .informativeTypes("informative1", "informative2")
            .build();

    assertThat(dataTypes.getAllDeclaredTypes())
        .containsExactly("authoritative1", "authoritative2", "informative1", "informative2");
  }

  @Test
  void noTypes() {
    CachingAgentDataTypes.Builder dataTypesBuilder = CachingAgentDataTypes.builder();
    assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(dataTypesBuilder::build);
  }

  @Test
  void onlyOnDemandType() {
    CachingAgentDataTypes.Builder dataTypesBuilder =
        CachingAgentDataTypes.builder().onDemandType("onDemand");
    assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(dataTypesBuilder::build);
  }

  @Test
  void overlappingTypes() {

    CachingAgentDataTypes.Builder dataTypesBuilder = CachingAgentDataTypes.builder();
    dataTypesBuilder.authoritativeTypes("type").informativeTypes("type");
    assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(dataTypesBuilder::build);

    dataTypesBuilder = CachingAgentDataTypes.builder();
    dataTypesBuilder.authoritativeTypes("type").onDemandType("type");
    assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(dataTypesBuilder::build);

    dataTypesBuilder = CachingAgentDataTypes.builder();
    dataTypesBuilder.informativeTypes("type").onDemandType("type");
    assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(dataTypesBuilder::build);
  }
}
