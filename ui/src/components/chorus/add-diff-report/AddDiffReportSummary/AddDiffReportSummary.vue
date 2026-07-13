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
  import { storeToRefs } from 'pinia';
  import { computed } from 'vue';
  import { useI18n } from 'vue-i18n';
  import {
    CDescriptionItem,
    CDescriptionList,
    CIcon,
    CTag,
  } from '@clyso/clyso-ui-kit';
  import { useChorusAddDiffReportStore } from '@/stores/chorusAddDiffReportStore';
  import ChorusStorageCard from '@/components/chorus/common/ChorusStorageCard/ChorusStorageCard.vue';
  import i18nAddDiffReport from '@/components/chorus/add-diff-report/i18nAddDiffReport';
  import { IconName } from '@/utils/types/icon';

  const {
    fromStorage,
    toStorage,
    selectedUser,
    fromBucketName,
    toBucketName,
    checkOnlyLastVersions,
    ignoreEtags,
    ignoreSizes,
  } = storeToRefs(useChorusAddDiffReportStore());

  const { t } = useI18n({ messages: i18nAddDiffReport });

  const activeOptions = computed(() =>
    [
      { key: 'checkOnlyLastVersions', value: checkOnlyLastVersions },
      { key: 'ignoreEtags', value: ignoreEtags },
      { key: 'ignoreSizes', value: ignoreSizes },
    ]
      .filter((option) => option.value.value)
      .map((option) => ({
        labelKey: `${option.key}Label`,
      })),
  );
</script>

<template>
  <div class="add-diff-report-summary">
    <CDescriptionList
      size="medium"
      class="summary-list"
      label-placement="top"
      :columns="1"
    >
      <CDescriptionItem
        v-if="fromStorage && toStorage"
        class="summary-list__item"
      >
        <template #label>{{ t('summaryStorages') }}:</template>

        <div class="storages">
          <ChorusStorageCard
            :storage="fromStorage"
            size="small"
            class="storages__storage-card"
            :type="fromStorage.isMain ? 'success' : 'warning'"
          />
          <CIcon
            class="storages__arrow"
            :is-inline="true"
            :name="IconName.BASE_ARROW_FORWARD"
          />
          <ChorusStorageCard
            :storage="toStorage"
            size="small"
            class="storages__storage-card"
            :type="toStorage.isMain ? 'success' : 'warning'"
          />
        </div>
      </CDescriptionItem>

      <CDescriptionItem class="summary-list__item">
        <template #label>{{ t('summaryBuckets') }}:</template>

        <div class="buckets">
          <CTag
            round
            size="small"
            type="info"
            :bordered="false"
            class="buckets__bucket-tag"
          >
            {{ fromBucketName }}
          </CTag>
          <CIcon
            class="buckets__arrow"
            :is-inline="true"
            :name="IconName.BASE_ARROW_FORWARD"
          />
          <CTag
            round
            size="small"
            type="info"
            :bordered="false"
            class="buckets__bucket-tag"
          >
            {{ toBucketName }}
          </CTag>
        </div>
      </CDescriptionItem>

      <CDescriptionItem class="summary-list__item">
        <template #label>{{ t('summaryUser') }}:</template>
        <CTag
          round
          size="small"
          type="info"
          :bordered="false"
        >
          {{ selectedUser }}
        </CTag>
      </CDescriptionItem>

      <CDescriptionItem
        v-if="activeOptions.length > 0"
        class="summary-list__item"
      >
        <template #label>{{ t('summaryOptions') }}:</template>

        <div class="options">
          <CTag
            v-for="option in activeOptions"
            :key="option.labelKey"
            round
            size="small"
            type="success"
            :bordered="false"
          >
            {{ t(option.labelKey) }}
          </CTag>
        </div>
      </CDescriptionItem>
    </CDescriptionList>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .storages,
  .buckets {
    display: flex;
    align-items: center;
    gap: utils.unit(2);

    &__storage-card {
      flex-grow: 1;
      align-self: stretch;
    }
  }

  .options {
    display: flex;
    gap: utils.unit(2);
  }
</style>
