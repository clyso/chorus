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
  import { CTile } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import i18nDiffReports from '@/components/chorus/diff-reports/i18nDiffReports';
  import DiffReportsList from '@/components/chorus/diff-reports/DiffReportsList/DiffReportsList.vue';
  import DiffReportsFilters from '@/components/chorus/diff-reports/DiffReportsFilters/DiffReportsFilters.vue';

  const { t } = useI18n({
    messages: i18nDiffReports,
  });
</script>

<template>
  <CTile class="diff-reports-tile">
    <template #title>
      {{ t('diffReportTitle') }}
    </template>

    <DiffReportsFilters class="diff-reports-tile__filters" />

    <DiffReportsList class="diff-reports-tile__diff-reports" />
  </CTile>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .diff-reports-tile {
    &__filters {
      margin-bottom: utils.unit(6);
    }
  }
</style>
