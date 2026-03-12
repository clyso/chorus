<!--
  - Copyright © 2026 Clyso GmbH
  -
  -  Licensed under the GNU Affero General Public License, Version 3.0 (the "License");
  -  you may not use this file except in compliance with the License.
  -  You may obtain a copy of the License at
  -
  -  https://www.gnu.org/licenses/agpl-3.0.html
  -
  -  Unless required by applicable law or agreed to in writing, software
  -  distributed under the License is distributed on an "AS IS" BASIS,
  -  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  -  See the License for the specific language governing permissions and
  -  limitations under the License.
  -->

<script setup lang="ts">
  import { useI18n } from 'vue-i18n';
  import { storeToRefs } from 'pinia';
  import {
    CDescriptionList,
    CDescriptionItem,
    CTag,
    CIcon,
  } from '@clyso/clyso-ui-kit';
  import i18nAddRoutingPolicy from '../i18nAddRoutingPolicy';
  import ChorusUserCard from '../../common/ChorusUserCard/ChorusUserCard.vue';
  import ChorusStorageCard from '../../common/ChorusStorageCard/ChorusStorageCard.vue';
  import { useChorusAddRoutingPolicyStore } from '@/stores/chorusAddRoutingPolicyStore';
  import { IconName } from '@/utils/types/icon';

  const { t } = useI18n({
    messages: i18nAddRoutingPolicy,
  });

  const {
    selectedUser,
    selectedToStorage,
    bucketName,
    isForAllBuckets,
    isBlocked,
  } = storeToRefs(useChorusAddRoutingPolicyStore());
</script>

<template>
  <div class="add-routing-policy-summary">
    <CDescriptionList
      size="medium"
      class="summary-list"
      label-placement="top"
      :columns="1"
    >
      <CDescriptionItem
        v-if="selectedUser"
        class="summary-list__item"
      >
        <template #label> {{ t('summaryColumnUser') }}: </template>

        <ChorusUserCard
          class="summary-list__user-card"
          :user="selectedUser"
        />
      </CDescriptionItem>

      <CDescriptionItem class="summary-list__item">
        <template #label> {{ t('summaryColumnStorage') }}: </template>

        <ChorusStorageCard
          v-if="selectedToStorage"
          class="summary-list__storage-card"
          :storage="selectedToStorage"
          size="small"
        />
        <CTag
          v-else
          size="small"
          round
          type="info"
        >
          {{ t('summaryNoStoragePlaceholder') }}
        </CTag>
      </CDescriptionItem>

      <CDescriptionItem class="summary-list__item">
        <template #label> {{ t('summaryColumnBucket') }}: </template>

        <div class="summary-list__bucket-item">
          <CTag
            round
            size="small"
            type="info"
          >
            {{ isForAllBuckets ? t('bucketSelectionAll') : bucketName }}
          </CTag>
        </div>
      </CDescriptionItem>

      <CDescriptionItem class="summary-list__item">
        <template #label> {{ t('summaryColumnStatus') }}: </template>

        <span class="summary-list__status-item">
          <CTag
            round
            size="small"
            :type="isBlocked ? 'error' : 'success'"
          >
            <template #icon>
              <CIcon
                :is-inline="true"
                :name="
                  isBlocked
                    ? IconName.BASE_CLOSE_CIRCLE
                    : IconName.BASE_CHECKMARK
                "
              />
            </template>
            {{
              isBlocked ? t('summaryStatusBlocked') : t('summaryStatusAllowed')
            }}
          </CTag>
        </span>
      </CDescriptionItem>
    </CDescriptionList>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .summary-list__user-card,
  .summary-list__storage-card {
    width: 50%;
    box-sizing: border-box;
  }

  .summary-list__item {
    margin-bottom: utils.unit(1);

    :deep(.c-description-item__content),
    :deep(.c-description-item__value) {
      width: 100%;
      display: block;
    }
  }
</style>
