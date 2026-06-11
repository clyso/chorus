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
  import { CTag } from '@clyso/clyso-ui-kit';
  import { computed } from 'vue';
  import i18nDiffReports from '@/components/chorus/diff-reports/i18nDiffReports';
  import type { DiffReport } from '@/utils/types/chorus';

  const props = defineProps<{
    report: DiffReport;
  }>();

  const { t } = useI18n({
    messages: i18nDiffReports,
  });

  const isReady = computed<boolean>(() => props.report.ready);
  const isConsistent = computed<boolean>(() => props.report.consistent);
</script>

<template>
  <div class="diff-reports-status-cell">
    <CTag
      v-if="!isReady"
      :bordered="false"
      size="small"
      type="info"
      round
    >
      {{ t('statusChecking') }}
    </CTag>
    <CTag
      v-else-if="isConsistent"
      :bordered="false"
      size="small"
      type="success"
      round
    >
      {{ t('statusConsistent') }}
    </CTag>
    <CTag
      v-else
      :bordered="false"
      size="small"
      type="error"
      round
    >
      {{ t('statusInconsistent') }}
    </CTag>
  </div>
</template>
