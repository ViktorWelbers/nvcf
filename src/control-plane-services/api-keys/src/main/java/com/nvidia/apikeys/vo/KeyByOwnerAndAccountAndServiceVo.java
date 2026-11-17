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
package com.nvidia.apikeys.vo;

import com.nvidia.apikeys.persistance.models.KeyByOwnerAndAccountAndServiceModel;
import com.nvidia.boot.jwt.services.mapping.annotation.ValueObject;
import java.time.Instant;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder(toBuilder = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
@ValueObject(model = KeyByOwnerAndAccountAndServiceModel.class)
public class KeyByOwnerAndAccountAndServiceVo {

    private String ncaId;
    private String ncaIdInDetails;
    private KeyOwnerType ownerType;
    private String ownerId;

    private String issuerServiceId;
    private String keyId;
    private KeyStatus keyStatus;
    private Instant createdAt;
    private Instant expiresAt;
    private Instant deletesAt;

    private String keyHash;
    private String apiKeySuffix;
    private String description;
    private Set<String> audienceServiceIds;

    public KeyByOwnerAndAccountAndServiceVo(KeyByOwnerAndServiceVo key, String ncaId) {
        this.ncaId = ncaId;
        this.ncaIdInDetails = key.getNcaIdInDetails();
        this.ownerType = key.getOwnerType();
        this.ownerId = key.getOwnerId();

        this.issuerServiceId = key.getIssuerServiceId();
        this.keyId = key.getKeyId();
        this.keyStatus = key.getKeyStatus();
        this.createdAt = key.getCreatedAt();
        this.expiresAt = key.getExpiresAt();
        this.deletesAt = key.getDeletesAt();

        this.keyHash = key.getKeyHash();
        this.apiKeySuffix = key.getApiKeySuffix();
        this.description = key.getDescription();
        this.audienceServiceIds = key.getAudienceServiceIds();
    }
}
