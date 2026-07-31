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
  import { CProgress, CTooltip, I18nLocale } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import type { DiffReport } from '@/utils/types/chorus';

  const { t } = useI18n({
    messages: {
      [I18nLocale.EN]: {
        detailsPopupDefault: 'Diff Report Status Details',
        detailsPopupDefaultFix: 'Diff Report Fix Status Details',
      },
      [I18nLocale.DE]: {
        detailsPopupDefault: 'Diff-Report Status Details',
        detailsPopupDefaultFix: 'Diff-Report Fix Status Details',
      },
    },
  });

  const props = withDefaults(
    defineProps<{
      report: DiffReport;
      type?: 'diff' | 'fix';
    }>(),
    {
      type: 'diff',
    },
  );

  const queued = computed<number>(() =>
    props.type === 'diff'
      ? Number(props.report.queued)
      : Number(props.report.fixQueued),
  );
  const completed = computed<number>(() =>
    props.type === 'diff'
      ? Number(props.report.completed)
      : Number(props.report.fixCompleted),
  );
  const ready = computed<boolean>(() =>
    props.type === 'diff'
      ? Boolean(props.report.ready)
      : Boolean(props.report.fixReady),
  );

  const percentage = computed<number>(() => {
    if (queued.value === 0) {
      return ready.value ? 100 : 0;
    }

    const pct = Math.ceil((completed.value / queued.value) * 100);

    return ready.value ? pct : Math.min(pct, 99);
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
        {{
          props.type === 'diff'
            ? t('detailsPopupDefault')
            : t('detailsPopupDefaultFix')
        }}:
      </span>
      {{ completed }} / {{ queued }}
    </div>
  </CTooltip>
</template>
