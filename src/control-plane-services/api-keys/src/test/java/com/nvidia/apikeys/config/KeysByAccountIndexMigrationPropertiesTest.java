/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.apikeys.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import jakarta.validation.Validation;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

class KeysByAccountIndexMigrationPropertiesTest {
    @Test
    void bindsOperatorSettingsAndKeepsSafeDefaults() {
        var binder = new Binder(new MapConfigurationPropertySource(Map.of(
                "apikeys.migration.keys-by-account-index.page-size", "17",
                "apikeys.migration.keys-by-account-index.fixed-delay", "2m",
                "apikeys.migration.keys-by-account-index.lock-consistency", "QUORUM",
                "apikeys.migration.keys-by-account-index.lock-serial-consistency", "SERIAL")));
        var properties = binder.bind("apikeys", Bindable.of(NakProperties.class)).orElseThrow(IllegalStateException::new);
        var config = properties.getMigration().getKeysByAccountIndex();
        assertThat(config.getPageSize()).isEqualTo(17);
        assertThat(config.getFixedDelay()).isEqualTo(Duration.ofMinutes(2));
        assertThat(config.getLockConsistency()).isEqualTo(DefaultConsistencyLevel.QUORUM);
        assertThat(config.getLockSerialConsistency()).isEqualTo(DefaultConsistencyLevel.SERIAL);
        assertThat(config.isEnabled()).isTrue();
        assertThat(config.getInitialDelay()).isEqualTo(Duration.ofMinutes(30));
        assertThat(config.getOrphanMinAge()).isEqualTo(Duration.ofMinutes(5));
        assertValid(config);
    }

    @Test
    void rejectsUnsafeDelaysLockDurationsAndRetryLimits() {
        try (var factory = Validation.buildDefaultValidatorFactory()) {
            var validator = factory.getValidator();
            var config = new KeysByAccountIndexMigrationProperties();
            config.setOrphanMinAge(Duration.ZERO);
            assertThat(validator.validate(config)).isNotEmpty();
            config.setOrphanMinAge(Duration.ofMinutes(5));
            config.setSettleDelay(Duration.ofSeconds(-1));
            assertThat(validator.validate(config)).isNotEmpty();
            config.setSettleDelay(Duration.ofMinutes(5));
            config.setLockAtMostFor(config.getLockAtLeastFor());
            assertThat(validator.validate(config)).isNotEmpty();
            config.setLockAtMostFor(Duration.ofMinutes(5));
            config.setPageFetchAttempts(0);
            assertThat(validator.validate(config)).isNotEmpty();
            config.setPageFetchAttempts(3);
            config.setLockSerialConsistency(DefaultConsistencyLevel.ONE);
            assertThat(validator.validate(config)).isNotEmpty();
        }
    }

    private void assertValid(KeysByAccountIndexMigrationProperties config) {
        try (var factory = Validation.buildDefaultValidatorFactory()) {
            assertThat(factory.getValidator().validate(config)).isEmpty();
        }
    }
}
