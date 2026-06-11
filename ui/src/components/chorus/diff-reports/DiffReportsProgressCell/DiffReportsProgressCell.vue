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
  import { CProgress, CTooltip } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import type { DiffReport } from '@/utils/types/chorus';
  import i18nDiffReports from '@/components/chorus/diff-reports/i18nDiffReports';

  const { t } = useI18n({
    messages: i18nDiffReports,
  });

  const props = defineProps<{
    report: DiffReport;
  }>();

  const percentage = computed<number>(() => {
    const { queued, completed, ready } = props.report;

    if (+queued === 0) {
      return ready ? 100 : 0;
    }

    const pct = Math.ceil((+completed / +queued) * 100);

    return ready ? pct : Math.min(pct, 99);
  });
</script>

<template>
  <CTooltip>
    <template #trigger>
      <CProgress
        type="line"
        :percentage="percentage"
        size="small"
        :height="2"
        :processing="percentage !== 100"
        indicator-placement="outside"
      />
    </template>

    <div class="details-popup">
      <span class="details-popup__title">
        {{ t('diffReportStatusDetails') }}:
      </span>
      {{ report.completed }} / {{ report.queued }}
    </div>
  </CTooltip>
</template>
