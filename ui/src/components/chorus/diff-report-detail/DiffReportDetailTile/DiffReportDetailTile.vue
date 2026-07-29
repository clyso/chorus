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

  const store = useChorusDiffReportDetailStore();
  const { initDiffReportDetailsPage } = store;
  const { hasError, isNotFound, locations } = storeToRefs(store);
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
      <!-- Content will be added in Step 2 -->
    </template>
  </CTile>
</template>

<style lang="scss" scoped>
  .diff-report-detail-tile {
    min-height: 400px;
  }
</style>
