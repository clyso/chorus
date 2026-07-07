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
  import { RouteName } from '@/utils/types/router';
  import { useChorusAddDiffReportStore } from '@/stores/chorusAddDiffReportStore';
  import i18nAddDiffReport from '@/components/chorus/add-diff-report/i18nAddDiffReport';

  const { t } = useI18n({ messages: i18nAddDiffReport });
  const { initAddDiffReportPage, $reset } = useChorusAddDiffReportStore();

  onBeforeMount(() => {
    initAddDiffReportPage();
  });

  onUnmounted(() => {
    $reset();
  });
</script>

<template>
  <CDashboardPage class="chorus-add-diff-report-page">
    <template #breadcrumbs>
      <CBreadcrumb>
        <CBreadcrumbItem :to="{ name: RouteName.CHORUS_DIFF_REPORTS }">
          {{ t('breadcrumbDiffReports') }}
        </CBreadcrumbItem>
        <CBreadcrumbItem :is-active="true">
          {{ t('breadcrumbCreateDiffReport') }}
        </CBreadcrumbItem>
      </CBreadcrumb>
    </template>
  </CDashboardPage>
</template>
