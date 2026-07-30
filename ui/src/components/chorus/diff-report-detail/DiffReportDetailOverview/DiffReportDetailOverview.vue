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
  import {
    CDescriptionItem,
    CDescriptionList,
    CTag,
    CIcon,
    CSkeleton,
  } from '@clyso/clyso-ui-kit';
  import { storeToRefs } from 'pinia';
  import { computed } from 'vue';
  import { IconName } from '@/utils/types/icon';
  import { useChorusDiffReportDetailStore } from '@/stores/chorusDiffReportDetailStore';
  import i18nDiffReportDetail from '@/components/chorus/diff-report-detail/i18nDiffReportDetail';

  const store = useChorusDiffReportDetailStore();
  const { report } = storeToRefs(store);
  const { t } = useI18n({ messages: i18nDiffReportDetail });

  type CTagProps = InstanceType<typeof CTag>['$props'];

  interface ReportItems {
    labelKey: string;
    tagValue: string;
    tagType: CTagProps['type'];
    tagIcon: string;
  }

  const reportValues = computed<ReportItems[] | undefined>(() => {
    if (!report.value) {
      return;
    }

    return [
      {
        labelKey: t('overviewStatus'),
        tagType: report.value.ready ? 'success' : 'warning',
        tagValue: report.value.ready ? t('statusReady') : t('statusChecking'),
        tagIcon: report.value.ready
          ? IconName.BASE_CHECKMARK
          : IconName.BASE_CLOSE_CIRCLE,
      },
      {
        labelKey: t('overviewConsistency'),
        tagType: report.value.consistent ? 'success' : 'error',
        tagValue: report.value.consistent
          ? t('statusConsistent')
          : t('statusInconsistent'),
        tagIcon: report.value.consistent
          ? IconName.BASE_CHECKMARK
          : IconName.BASE_CLOSE_CIRCLE,
      },
      {
        labelKey: t('overviewVersioned'),
        tagType: report.value.versioned ? 'success' : 'warning',
        tagValue: report.value.versioned
          ? t('configVersioned')
          : t('configNotVersioned'),
        tagIcon: report.value.versioned
          ? IconName.BASE_CHECKMARK
          : IconName.BASE_CLOSE_CIRCLE,
      },
      {
        labelKey: t('overviewEtags'),
        tagType: report.value.ignoreEtags ? 'warning' : 'success',
        tagValue: report.value.ignoreEtags
          ? t('configIgnoresEtags')
          : t('configConsidersEtags'),
        tagIcon: report.value.ignoreEtags
          ? IconName.BASE_CLOSE_CIRCLE
          : IconName.BASE_CHECKMARK,
      },
      {
        labelKey: t('overviewSizes'),
        tagType: report.value.ignoreSizes ? 'warning' : 'success',
        tagValue: report.value.ignoreSizes
          ? t('configIgnoresSizes')
          : t('configConsidersSizes'),
        tagIcon: report.value.ignoreSizes
          ? IconName.BASE_CLOSE_CIRCLE
          : IconName.BASE_CHECKMARK,
      },
    ];
  });
</script>

<template>
  <div class="diff-report-detail-overview">
    <h5>{{ t('overviewTitle') }}</h5>

    <template v-if="!report || !report.ready">
      <div class="diff-report-detail-overview__list">
        <CSkeleton
          v-for="index in 5"
          :key="index"
          :height="20"
          :width="500"
          :padding-block="4"
          :border-radius="8"
        />
      </div>
    </template>

    <CDescriptionList
      v-else
      label-placement="left"
      :columns="1"
      size="small"
      class="diff-report-detail-overview__list"
    >
      <CDescriptionItem
        v-for="(reportItem, index) in reportValues"
        :key="index"
      >
        <template #label>{{ reportItem.labelKey }}: </template>
        <CTag
          :bordered="false"
          round
          size="small"
          :type="reportItem.tagType"
        >
          <template #icon>
            <CIcon
              :is-inline="true"
              :name="reportItem.tagIcon"
            />
          </template>
          {{ reportItem.tagValue }}
        </CTag>
      </CDescriptionItem>
    </CDescriptionList>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .diff-report-detail-overview {
    h5 {
      margin-bottom: utils.unit(3);
    }

    &__list {
      gap: utils.unit(3);
    }

    :deep(.c-description-item__label) {
      min-width: 120px;
      color: var(--text-color-3);
    }

    :deep(.c-icon) {
      width: 12px;
      height: 12px;
    }
  }
</style>
