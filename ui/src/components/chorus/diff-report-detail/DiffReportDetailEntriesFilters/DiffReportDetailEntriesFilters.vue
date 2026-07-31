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
  import { CInput, CSkeleton } from '@clyso/clyso-ui-kit';
  import { storeToRefs } from 'pinia';
  import { useI18n } from 'vue-i18n';
  import { useChorusDiffReportEntriesStore } from '@/stores/chorusDiffReportEntriesStore';
  import i18nDiffReportDetail from '@/components/chorus/diff-report-detail/i18nDiffReportDetail';
  import { useChorusDiffReportDetailStore } from '@/stores/chorusDiffReportDetailStore';

  const { t } = useI18n({ messages: i18nDiffReportDetail });
  const { filterObjectName, isLoading } = storeToRefs(
    useChorusDiffReportEntriesStore(),
  );
  const { report } = storeToRefs(useChorusDiffReportDetailStore());
</script>

<template>
  <div class="diff-report-detail-entries-filters">
    <div
      v-if="!report || !report.ready || isLoading"
      key="loading"
      class="diff-report-detail-entries-filters__list"
    >
      <CSkeleton
        :height="34"
        :border-radius="4"
      />
    </div>
    <div
      v-else
      key="filters"
      class="diff-report-detail-entries-filters__list"
    >
      <CInput
        v-model:value="filterObjectName"
        :placeholder="t('entriesSearchPlaceholder')"
        clearable
        class="diff-report-detail-entries-filters__search"
      />
    </div>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .diff-report-detail-entries-filters {
    padding: 24px 16px;
    border-radius: 12px;
    background-color: var(--filters-card-color);
    border: 1px solid var(--border-color);

    @include utils.mobile {
      padding: 0;
      border-radius: 0;
      background-color: unset;
      border: 0;
    }

    &__list {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
      gap: utils.unit(5) utils.unit(3);
      align-items: start;
    }
  }
</style>
