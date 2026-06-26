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
  import { computed, h } from 'vue';
  import { useI18n } from 'vue-i18n';
  import {
    CButton,
    CIcon,
    CTooltip,
    CBadge,
    useDialog,
  } from '@clyso/clyso-ui-kit';
  import { useChorusDiffReportsStore } from '@/stores/chorusDiffReportsStore';
  import i18nDiffReports from '@/components/chorus/diff-reports/i18nDiffReports';
  import { IconName } from '@/utils/types/icon';
  import { useChorusNotification } from '@/utils/composables/useChorusNotification';
  import DiffReportsShortList from '@/components/chorus/diff-reports/DiffReportsShortList/DiffReportsShortList.vue';
  import { DiffReportsHelper } from '@/utils/helpers/DiffReportsHelper';
  import type { DiffReport } from '@/utils/types/chorus';
  import type { AddId } from '@/utils/types/helper';

  const { t } = useI18n({ messages: i18nDiffReports });

  const {
    selectedReportsCount,
    isAnyReportsSelected,
    selectedReports,
    isDeleteSelectedProcessing,
  } = storeToRefs(useChorusDiffReportsStore());

  const { deleteDiffReports: storeDeleteDiffReports } =
    useChorusDiffReportsStore();
  const { createNotification } = useChorusNotification();
  const { createDialog } = useDialog();

  /**
   * Filter out reports with more than 2 locations. These have to be handled on
   * CLI.
   */
  const selectedDiffReportsForDelete = computed(() =>
    selectedReports.value.filter((report) =>
      DiffReportsHelper.isTwoLocationReport(report),
    ),
  );

  const isDeleteDisabled = computed(
    () =>
      !isAnyReportsSelected.value ||
      selectedDiffReportsForDelete.value.length === 0,
  );

  async function deleteDiffReports(
    reports: AddId<DiffReport>[] = selectedDiffReportsForDelete.value,
  ) {
    const { successList, errorList } = await storeDeleteDiffReports(reports);

    if (successList.length > 0) {
      createNotification({
        type: 'success',
        title: t('deleteDiffReportsSuccessTitle'),
        duration: 4000,
        content: () =>
          h('div', [
            t('deleteDiffReportsSuccessContent', { total: successList.length }),
            h(DiffReportsShortList, { reports: successList }),
          ]),
      });
    }

    if (errorList.length > 0) {
      createNotification({
        type: 'error',
        title: t('deleteDiffReportsErrorTitle'),
        positiveText: t('deleteDiffReportsErrorAction'),
        positiveHandler: () => {
          deleteDiffReports(errorList);
        },
        content: () =>
          h('div', [
            t('deleteDiffReportsErrorContent', { total: errorList.length }),
            h(DiffReportsShortList, { reports: errorList }),
          ]),
      });
    }
  }

  function handleDiffReportsDelete() {
    createDialog({
      type: 'error',
      iconName: IconName.BASE_TRASH,
      title: t('deleteDiffReportsConfirmTitle'),
      content: () => [
        h(
          'div',
          { style: 'margin-bottom: 8px' },
          t('deleteDiffReportsConfirmContent'),
        ),
        h(DiffReportsShortList, {
          reports: selectedDiffReportsForDelete.value,
          size: 'medium',
          style: 'margin-bottom: 8px',
        }),
        t('deleteDiffReportsConfirmQuestion'),
      ],
      positiveText: t('deleteDiffReportsConfirmAction'),
      negativeText: t('deleteDiffReportsCancelAction'),
      positiveHandler: () => deleteDiffReports(),
    });
  }
</script>

<template>
  <div class="diff-reports-list-actions">
    <div class="diff-reports-list-actions__selection-actions">
      <CTooltip :delay="1000">
        <template #trigger>
          <CBadge
            :offset="[-4, 0]"
            :value="selectedDiffReportsForDelete.length"
            :max="100"
          >
            <CButton
              secondary
              :disabled="isDeleteDisabled"
              :loading="isDeleteSelectedProcessing"
              size="medium"
              type="error"
              @click="handleDiffReportsDelete"
            >
              <template #icon>
                <CIcon
                  :is-inline="true"
                  :name="IconName.BASE_TRASH"
                />
              </template>
            </CButton>
          </CBadge>
        </template>

        <template v-if="!isAnyReportsSelected">
          {{ t('deleteDiffReportsDeleteAction') }}
        </template>
        <template v-else>
          {{ t('deleteDiffReportsSelected', { total: selectedReportsCount }) }}
        </template>
      </CTooltip>
    </div>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .diff-reports-list-actions {
    display: flex;
    justify-content: space-between;
    gap: utils.unit(2);

    &__selection-actions {
      display: inline-flex;
      align-items: center;
      gap: utils.unit(3);

      ::v-deep(.c-badge-sup) {
        pointer-events: none;
      }
    }
  }
</style>
