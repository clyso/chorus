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
  import { useI18n } from 'vue-i18n';
  import { computed } from 'vue';
  import { CResult, CTile } from '@clyso/clyso-ui-kit';
  import { useRouter } from 'vue-router';
  import { useChorusDiffReportDetailStore } from '@/stores/chorusDiffReportDetailStore';
  import i18nDiffReportDetail from '@/components/chorus/diff-report-detail/i18nDiffReportDetail';
  import { RouteName } from '@/utils/types/router';
  import DiffReportDetailHeader from '@/components/chorus/diff-report-detail/DiffReportDetailHeader/DiffReportDetailHeader.vue';
  import DiffReportDetailOverview from '@/components/chorus/diff-report-detail/DiffReportDetailOverview/DiffReportDetailOverview.vue';
  import DiffReportDetailProgress from '@/components/chorus/diff-report-detail/DiffReportDetailProgress/DiffReportDetailProgress.vue';

  const store = useChorusDiffReportDetailStore();
  const { initDiffReportDetailsPage } = store;
  const { hasError, isNotFound, locations, hasFixActivity } =
    storeToRefs(store);
  const { t } = useI18n({ messages: i18nDiffReportDetail });
  const router = useRouter();

  const errorType = computed(() => {
    if (locations.value.length !== 2) return 'invalidLocations';

    if (isNotFound.value) return 'notFound';

    if (hasError.value) return 'error';

    return null;
  });

  function handleBackToList() {
    router.push({ name: RouteName.CHORUS_DIFF_REPORTS });
  }
</script>

<template>
  <CTile class="diff-report-detail-tile">
    <CResult
      v-if="errorType"
      status="error"
      type="error"
      size="large"
      :max-width="600"
      @positive-click="
        errorType === 'error' ? initDiffReportDetailsPage() : handleBackToList()
      "
      class="diff-report-detail-tile__error"
    >
      <template #title>
        {{ t(`${errorType}Title`) }}
      </template>

      <p>{{ t(`${errorType}Text`) }}</p>

      <template #positive-text>
        {{ t(`${errorType}Action`) }}
      </template>
    </CResult>

    <template v-else>
      <div class="diff-report-detail-tile__content">
        <DiffReportDetailHeader />
        <DiffReportDetailOverview />
        <div class="diff-report-detail-tile__progress-row">
          <DiffReportDetailProgress
            type="diff"
            class="diff-report-detail-tile__progress-item"
          />
          <DiffReportDetailProgress
            v-if="hasFixActivity"
            type="fix"
            class="diff-report-detail-tile__progress-item"
          />
        </div>
      </div>
    </template>
  </CTile>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .diff-report-detail-tile {
    min-height: 400px;

    &__content {
      display: grid;
      gap: utils.unit(8);
    }

    &__progress-item {
      flex: 1;
      max-width: 500px;
    }

    &__progress-row {
      display: flex;

      @include utils.mobile {
        flex-direction: column;
        gap: utils.unit(6);
      }
    }
  }
</style>
