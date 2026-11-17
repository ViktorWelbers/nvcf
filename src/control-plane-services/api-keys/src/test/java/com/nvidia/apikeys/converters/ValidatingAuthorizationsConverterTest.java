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

package com.nvidia.apikeys.converters;

import static com.nvidia.apikeys.TestData.AUTHORIZATION_JSON_NODES_1;
import static com.nvidia.apikeys.TestData.AUTHORIZATION_JSON_NODES_2;
import static com.nvidia.apikeys.TestData.KEY_AUTHZ_1;
import static com.nvidia.apikeys.TestData.KEY_AUTHZ_2;
import static com.nvidia.apikeys.TestData.SERVICE_VO_1;
import static com.nvidia.apikeys.TestData.TEST_OM;
import static com.nvidia.apikeys.TestData.servicesService;
import static com.nvidia.apikeys.utils.TestUtils.assertThrowsExceptionWithDetails;
import static org.assertj.core.api.Assertions.assertThat;

import tools.jackson.databind.node.ObjectNode;
import com.nvidia.apikeys.services.ServicesService;
import com.nvidia.apikeys.vo.ServiceVo;
import com.nvidia.boot.exceptions.BadRequestException;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ValidatingAuthorizationsConverterTest {

    private static final String LEGACY_ID = "legacy-service-id";
    private static final String CANONICAL_ID = "canonical-service-id";

    @Spy
    private ServicesService servicesService = servicesService(
            SERVICE_VO_1.toBuilder()
                    .serviceId(CANONICAL_ID)
                    .legacyServiceIds(Set.of(LEGACY_ID))
                    .build());

    @InjectMocks
    private ValidatingAuthorizationsConverter converter;

    @Test
    void getValidatedRequest_parsesAuthorization() {
        assertThat(converter.readValidAuthorizations(SERVICE_VO_1, AUTHORIZATION_JSON_NODES_1))
                .isEqualTo(KEY_AUTHZ_1);
        assertThat(converter.readValidAuthorizations(SERVICE_VO_1, AUTHORIZATION_JSON_NODES_2))
                .isEqualTo(KEY_AUTHZ_2);
    }

    @Test
    void getValidatedRequest_parseThrowsIfAuthzNull() {
        assertThrowsExceptionWithDetails(
                BadRequestException.class,
                () -> converter.readValidAuthorizations(SERVICE_VO_1, null),
                "authorizations can not be null");
    }

    @Test
    void getValidatedRequest_parseThrowsIfAuthzTooLong() {
        ObjectNode authzNode = TEST_OM.createObjectNode()
                .put("key", "0123456789");

        ServiceVo service = ServiceVo.builder()
                .maxAuthzSizeChars(10)
                .build();

        assertThrowsExceptionWithDetails(
                BadRequestException.class,
                () -> converter.readValidAuthorizations(service, authzNode),
                "authorizations size can not be greater than 10 but was 20");
    }

    @Test
    void readValidAuthorizations_canonicalizesAudAndPreservesAuds() {
        var authz = TEST_OM.readTree("""
                {"policies": [{"aud": "legacy-service-id", "auds": ["legacy-service-id"], "product": "p"}]}
                """);

        assertThat(converter.readValidAuthorizations(SERVICE_VO_1, authz))
                .isEqualTo("{\"policies\":[{\"aud\":\"canonical-service-id\","
                                   + "\"auds\":[\"legacy-service-id\"],\"product\":\"p\"}]}");
    }

    @Test
    void readValidAuthorizations_preservesCanonicalAudienceAndOtherPolicyFields() {
        var authz = TEST_OM.readTree("""
                {"policies": [{"aud": "canonical-service-id", "auds": ["other-service", 7]}]}
                """);

        assertThat(converter.readValidAuthorizations(SERVICE_VO_1, authz))
                .isEqualTo("{\"policies\":[{\"aud\":\"canonical-service-id\","
                                   + "\"auds\":[\"other-service\",7]}]}");
    }

    @Test
    void readValidAuthorizations_doesNotChangeTheCallersNode() {
        var authz = TEST_OM.readTree("""
                {"policies": [{"aud": "legacy-service-id"}]}
                """);

        converter.readValidAuthorizations(SERVICE_VO_1, authz);

        assertThat(authz.toString()).isEqualTo("{\"policies\":[{\"aud\":\"legacy-service-id\"}]}");
    }
}
