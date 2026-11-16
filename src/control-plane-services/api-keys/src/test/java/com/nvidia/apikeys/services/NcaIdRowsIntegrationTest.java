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

package com.nvidia.apikeys.services;

import static com.nvidia.apikeys.TestData.API_KEY_HASH_1;
import static com.nvidia.apikeys.TestData.KEY_OWNER_VO_1;
import static com.nvidia.apikeys.TestData.KEY_VO_1;
import static com.nvidia.apikeys.config.IntegrationTestConfiguration.KEY_SPACE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastax.driver.core.Session;
import com.nvidia.apikeys.App;
import com.nvidia.apikeys.caching.CachingKeysLoader;
import com.nvidia.apikeys.config.IntegrationTestConfiguration;
import com.nvidia.apikeys.persistance.dao.KeyByOwnerAndServiceModelConverter;
import com.nvidia.apikeys.persistance.dao.KeyModelConverter;
import com.nvidia.apikeys.persistance.dao.KeysDao;
import com.nvidia.apikeys.vo.KeyByOwnerAndServiceVo;
import com.nvidia.boot.exceptions.NotFoundException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.test.context.ContextConfiguration;

@ExtendWith(IntegrationTestConfiguration.TestCleanerExtension.class)
@AutoConfigureTestRestTemplate
@SpringBootTest(
        classes = App.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.profiles.active:integrationtest")
@ContextConfiguration(initializers = IntegrationTestConfiguration.Initializer.class)
class NcaIdRowsIntegrationTest {

    @Autowired
    private KeysDao keysDao;

    @Autowired
    private CachingKeysLoader cachingKeysLoader;

    @Autowired
    private KeyModelConverter keyConverter;

    @Autowired
    private KeyByOwnerAndServiceModelConverter ownerKeyConverter;

    @Autowired
    private CassandraTemplate cassandraTemplate;

    @Test
    void ncaIdsRoundTripAndStayApart() {
        var key = KEY_VO_1.toBuilder().ncaId("column-nca").ncaIdInDetails("details-nca").build();

        cassandraTemplate.insert(keyConverter.voToModel(key));
        cassandraTemplate.insert(ownerKeyConverter.voToModel(
                new KeyByOwnerAndServiceVo(key, KEY_OWNER_VO_1)));

        var saved = keysDao.getKeyByHash(API_KEY_HASH_1).orElseThrow();
        var ownerKey = keysDao.getKeyByOwnerAndServiceAndId(
                key.getOwnerType(), key.getOwnerId(), key.getIssuerServiceId(), key.getKeyId())
                .orElseThrow();
        assertThat(saved.getNcaId()).isEqualTo("column-nca");
        assertThat(saved.getNcaIdInDetails()).isEqualTo("details-nca");
        assertThat(ownerKey.getNcaId()).isEqualTo("column-nca");
        assertThat(ownerKey.getNcaIdInDetails()).isEqualTo("details-nca");

        Session session = IntegrationTestConfiguration.CQL_SESSION;
        var row = session.execute("select nca_id from " + KEY_SPACE + ".keys where api_key_hash='"
                                          + API_KEY_HASH_1 + "'").one();
        assertThat(row.getString("nca_id")).isEqualTo("column-nca");
    }

    @Test
    void rowWithNcaIdButNoStatusOrDetailsIsNotFound() {
        Session session = IntegrationTestConfiguration.CQL_SESSION;
        session.execute("insert into " + KEY_SPACE + ".keys (api_key_hash, nca_id) values ('"
                                + API_KEY_HASH_1 + "', 'some-nca')");

        assertThat(keysDao.getKeyByHash(API_KEY_HASH_1)).isEmpty();
        assertThrows(NotFoundException.class,
                     () -> cachingKeysLoader.loadKeyByHash(API_KEY_HASH_1));
    }
}
