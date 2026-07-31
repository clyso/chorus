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
  import {
    CBreadcrumb,
    CBreadcrumbItem,
    CDashboardPage,
  } from '@clyso/clyso-ui-kit';
  import { onBeforeMount, onUnmounted } from 'vue';
  import { useI18n } from 'vue-i18n';
  import { storeToRefs } from 'pinia';
  import { RouteName } from '@/utils/types/router';
  import { useChorusDiffReportDetailStore } from '@/stores/chorusDiffReportDetailStore';
  import { useChorusDiffReportEntriesStore } from '@/stores/chorusDiffReportEntriesStore';
  import i18nDiffReportDetail from '@/components/chorus/diff-report-detail/i18nDiffReportDetail';
  import DiffReportDetailTile from '@/components/chorus/diff-report-detail/DiffReportDetailTile/DiffReportDetailTile.vue';

  const { t } = useI18n({ messages: i18nDiffReportDetail });

  const diffReportDetailStore = useChorusDiffReportDetailStore();
  const diffReportDetailEntriesStore = useChorusDiffReportEntriesStore();
  const { locations } = storeToRefs(diffReportDetailStore);

  onBeforeMount(() => {
    diffReportDetailStore.initDiffReportDetailsPage();
  });

  onUnmounted(() => {
    diffReportDetailStore.$reset();
    diffReportDetailEntriesStore.$reset();
  });
</script>

<template>
  <CDashboardPage class="chorus-diff-report-detail-page">
    <template #breadcrumbs>
      <CBreadcrumb>
        <CBreadcrumbItem :to="{ name: RouteName.CHORUS_DIFF_REPORTS }">
          {{ t('breadcrumbDiffReports') }}
        </CBreadcrumbItem>
        <CBreadcrumbItem :is-active="true">
          <span
            v-for="(location, index) in locations"
            :key="index"
          >
            {{ location.storage }}/{{ location.bucket }}
            <span v-if="index < locations.length - 1"> → </span>
          </span>
        </CBreadcrumbItem>
      </CBreadcrumb>
    </template>

    <DiffReportDetailTile />
  </CDashboardPage>
</template>

<style lang="scss" scoped>
  .chorus-diff-report-detail-page {
    height: 100%;
  }
</style>
