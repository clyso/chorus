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
  import { computed } from 'vue';
  import { useI18n } from 'vue-i18n';
  import {
    CDescriptionItem,
    CDescriptionList,
    CTag,
  } from '@clyso/clyso-ui-kit';
  import { storeToRefs } from 'pinia';
  import { useChorusDiffReportDetailStore } from '@/stores/chorusDiffReportDetailStore';
  import { useChorusDiffReportEntriesStore } from '@/stores/chorusDiffReportEntriesStore';
  import i18nDiffReportDetail from '@/components/chorus/diff-report-detail/i18nDiffReportDetail';
  import ChorusDiffReportsProgress from '@/components/chorus/common/ChorusDiffReportProgress/ChorusDiffReportsProgress.vue';

  const props = defineProps<{
    type: 'diff' | 'fix';
  }>();

  const detailStore = useChorusDiffReportDetailStore();
  const entriesStore = useChorusDiffReportEntriesStore();
  const { report } = storeToRefs(detailStore);
  const {
    inconsistentObjectsCount,
    hasError: hasEntriesStoreError,
    isLoading: isEntriesLoading,
  } = storeToRefs(entriesStore);

  const { t } = useI18n({ messages: i18nDiffReportDetail });

  interface DescriptionItem {
    label: string;
    value?: string | number | boolean;
  }

  const title = computed(() =>
    props.type === 'diff' ? t('diffProgressTitle') : t('fixProgressTitle'),
  );

  const items = computed<DescriptionItem[]>(() => {
    if (!report.value) return [];

    // Diff properties
    if (props.type === 'diff') {
      return [
        { label: t('progressQueued'), value: report.value.queued },
        { label: t('progressCompleted'), value: report.value.completed },
        {
          label: t('progressInconsistentObjects'),
          value:
            hasEntriesStoreError.value || isEntriesLoading.value
              ? '-'
              : inconsistentObjectsCount.value,
        },
      ];
    }

    // Fix properties
    return [
      { label: t('fixReady'), value: report.value.fixReady },
      { label: t('fixQueued'), value: report.value.fixQueued },
      { label: t('fixCompleted'), value: report.value.fixCompleted },
    ];
  });
</script>

<template>
  <div class="diff-report-detail-progress">
    <h5>{{ title }}</h5>

    <CDescriptionList
      v-if="report"
      label-placement="left"
      :columns="1"
      size="small"
      class="diff-report-detail-progress__list"
    >
      <CDescriptionItem class="diff-report-detail-progress__progress-bar">
        <template #label>{{ t('progressLabel') }}</template>
        <ChorusDiffReportsProgress
          :report="report"
          :type="type"
        />
      </CDescriptionItem>
      <CDescriptionItem
        v-for="(item, index) in items"
        :key="index"
      >
        <template #label>{{ item.label }}</template>
        <CTag
          v-if="typeof item.value === 'boolean'"
          :bordered="false"
          round
          size="small"
          :type="item.value ? 'success' : 'warning'"
        >
          {{ item.value ? t('fixReadyYes') : t('fixReadyNo') }}
        </CTag>
        <p v-else>
          {{ item.value }}
        </p>
      </CDescriptionItem>
    </CDescriptionList>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .diff-report-detail-progress {
    h5 {
      margin-bottom: utils.unit(3);
    }

    &__list {
      gap: utils.unit(3);
    }

    &__progress-bar {
      max-width: 400px;
    }

    :deep(.c-description-item__label) {
      min-width: 160px;
      color: var(--text-color-3);
    }
  }
</style>
