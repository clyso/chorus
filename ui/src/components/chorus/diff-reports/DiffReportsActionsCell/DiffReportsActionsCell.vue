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
  import { CButton, CIcon, CTooltip, useDialog } from '@clyso/clyso-ui-kit';
  import { h, ref } from 'vue';
  import { useI18n } from 'vue-i18n';
  import DiffReportsShortList from '@/components/chorus/diff-reports/DiffReportsShortList/DiffReportsShortList.vue';
  import i18nDiffReports from '@/components/chorus/diff-reports/i18nDiffReports';
  import { useChorusDiffReportsStore } from '@/stores/chorusDiffReportsStore';
  import { useChorusNotification } from '@/utils/composables/useChorusNotification';
  import type { DiffReport } from '@/utils/types/chorus';
  import type { AddId } from '@/utils/types/helper';
  import { IconName } from '@/utils/types/icon';

  const { t } = useI18n({ messages: i18nDiffReports });

  const { report } = defineProps<{
    report: AddId<DiffReport>;
  }>();

  const { deleteDiffReports, restartDiffReports } = useChorusDiffReportsStore();
  const { createNotification } = useChorusNotification();
  const { createDialog } = useDialog();
  const isDeleteLoading = ref(false);
  const isRestartLoading = ref(false);

  async function deleteDiffReport() {
    isDeleteLoading.value = true;

    const { successList, errorList } = await deleteDiffReports([report]);

    isDeleteLoading.value = false;

    if (successList.length > 0) {
      createNotification({
        type: 'success',
        title: t('deleteDiffReportSuccessTitle'),
        duration: 4000,
        content: () =>
          h('div', [
            t('deleteDiffReportSuccessContent'),
            h(DiffReportsShortList, { reports: successList }),
          ]),
      });
    }

    if (errorList.length > 0) {
      createNotification({
        type: 'error',
        title: t('deleteDiffReportErrorTitle'),
        positiveText: t('deleteDiffReportErrorAction'),
        positiveHandler: () => {
          deleteDiffReport();
        },
        content: () =>
          h('div', [
            t('deleteDiffReportErrorContent'),
            h(DiffReportsShortList, { reports: errorList }),
          ]),
      });
    }
  }

  async function restartDiffReport() {
    isRestartLoading.value = true;

    const { successList, errorList } = await restartDiffReports([report]);

    isRestartLoading.value = false;

    if (successList.length > 0) {
      createNotification({
        type: 'success',
        title: t('restartDiffReportSuccessTitle'),
        duration: 4000,
        content: () =>
          h('div', [
            t('restartDiffReportSuccessContent'),
            h(DiffReportsShortList, { reports: successList }),
          ]),
      });
    }

    if (errorList.length > 0) {
      createNotification({
        type: 'error',
        title: t('restartDiffReportErrorTitle'),
        positiveText: t('restartDiffReportErrorAction'),
        positiveHandler: () => {
          restartDiffReport();
        },
        content: () =>
          h('div', [
            t('restartDiffReportErrorContent'),
            h(DiffReportsShortList, { reports: errorList }),
          ]),
      });
    }
  }

  function handleDiffReportRestart() {
    createDialog({
      type: 'warning',
      iconName: IconName.BASE_REFRESH,
      title: t('restartDiffReportConfirmTitle'),
      content: () => [
        h(
          'div',
          { style: 'margin-bottom: 8px' },
          t('restartDiffReportConfirmContent'),
        ),
        h(DiffReportsShortList, {
          reports: [report],
          size: 'medium',
          style: 'margin-bottom: 8px',
        }),
        t('restartDiffReportConfirmQuestion'),
      ],
      positiveText: t('restartDiffReportConfirmAction'),
      negativeText: t('restartDiffReportCancelAction'),
      positiveHandler: () => restartDiffReport(),
    });
  }

  function handleDiffReportDelete() {
    createDialog({
      type: 'error',
      iconName: IconName.BASE_TRASH,
      title: t('deleteDiffReportConfirmTitle'),
      content: () => [
        h(
          'div',
          { style: 'margin-bottom: 8px' },
          t('deleteDiffReportConfirmContent'),
        ),
        h(DiffReportsShortList, {
          reports: [report],
          size: 'medium',
          style: 'margin-bottom: 8px',
        }),
        t('deleteDiffReportConfirmQuestion'),
      ],
      positiveText: t('deleteDiffReportConfirmAction'),
      negativeText: t('deleteDiffReportCancelAction'),
      positiveHandler: () => deleteDiffReport(),
    });
  }
</script>

<template>
  <div class="diff-reports-actions">
    <div class="diff-reports-actions__list">
      <div
        class="diff-reports-actions__item diff-reports-actions__item--restart"
      >
        <CTooltip :delay="1000">
          <template #trigger>
            <CButton
              secondary
              size="tiny"
              type="warning"
              :loading="isRestartLoading"
              @click="handleDiffReportRestart"
            >
              <template #icon>
                <CIcon
                  :is-inline="true"
                  :name="IconName.BASE_REFRESH"
                />
              </template>
            </CButton>
          </template>

          {{ t('restartDiffReportRestartAction') }}
        </CTooltip>
      </div>
      <div
        class="diff-reports-actions__item diff-reports-actions__item--delete"
      >
        <CTooltip :delay="1000">
          <template #trigger>
            <CButton
              secondary
              size="tiny"
              type="error"
              :loading="isDeleteLoading"
              @click="handleDiffReportDelete"
            >
              <template #icon>
                <CIcon
                  :is-inline="true"
                  :name="IconName.BASE_TRASH"
                />
              </template>
            </CButton>
          </template>

          {{ t('deleteDiffReportDeleteAction') }}
        </CTooltip>
      </div>
    </div>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .diff-reports-actions {
    .diff-reports-actions__list {
      display: flex;
      gap: utils.unit(1);
    }
  }
</style>
