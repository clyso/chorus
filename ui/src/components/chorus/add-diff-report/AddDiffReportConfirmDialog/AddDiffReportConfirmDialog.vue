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
  import { CDialog } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import { storeToRefs } from 'pinia';
  import { nextTick } from 'vue';
  import { useRouter } from 'vue-router';
  import i18nAddDiffReport from '@/components/chorus/add-diff-report/i18nAddDiffReport';
  import { useChorusAddDiffReportStore } from '@/stores/chorusAddDiffReportStore';
  import { useChorusNotification } from '@/utils/composables/useChorusNotification';
  import { RouteName } from '@/utils/types/router';
  import AddDiffReportSummary from '@/components/chorus/add-diff-report/AddDiffReportSummary/AddDiffReportSummary.vue';

  const { t } = useI18n({
    messages: i18nAddDiffReport,
  });

  const router = useRouter();
  const { isConfirmDialogOpen } = storeToRefs(useChorusAddDiffReportStore());
  const { createDiffReport: callCreateDiffReport } =
    useChorusAddDiffReportStore();
  const { createNotification, createRetryNotification } =
    useChorusNotification();

  async function createDiffReport() {
    try {
      await callCreateDiffReport();

      createNotification({
        type: 'success',
        title: t('successTitle'),
        content: t('createDiffReportSuccess'),
        duration: 4000,
      });

      isConfirmDialogOpen.value = false;

      await nextTick();

      router.push({ name: RouteName.CHORUS_DIFF_REPORTS });
    } catch (error: unknown) {
      createRetryNotification({
        title: t('errorTitle'),
        message: t('createDiffReportError'),
        error,
        positiveText: t('errorAction'),
        positiveHandler: () => {
          createDiffReport();
        },
      });
    }
  }
</script>

<template>
  <CDialog
    class="add-diff-report-confirm-dialog"
    type="confirm"
    :is-shown="isConfirmDialogOpen"
    :width="500"
    :positive-handler="createDiffReport"
    @update:is-shown="
      (value) => {
        isConfirmDialogOpen = value;
      }
    "
  >
    <template #title>
      {{ t('confirmDiffReportTitle') }}
    </template>

    <div class="confirmation-details">
      <p class="confirmation-details__description">
        {{ t('confirmDiffReportDescription') }}
      </p>

      <AddDiffReportSummary />
    </div>

    <template #positive-text>
      {{ t('confirmDiffReportPositive') }}
    </template>
    <template #negative-text>
      {{ t('confirmDiffReportNegative') }}
    </template>
  </CDialog>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .confirmation-details {
    &__description {
      margin-bottom: utils.unit(4);
    }
  }
</style>
