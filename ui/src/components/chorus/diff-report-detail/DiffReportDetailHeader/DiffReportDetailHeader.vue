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
  import { CButton, CIcon, CTag, useDialog } from '@clyso/clyso-ui-kit';
  import { storeToRefs } from 'pinia';
  import { useChorusDiffReportDetailStore } from '@/stores/chorusDiffReportDetailStore';
  import { useChorusNotification } from '@/utils/composables/useChorusNotification';
  import { IconName } from '@/utils/types/icon';
  import ChorusDirectionCell from '@/components/chorus/common/ChorusDirectionCell/ChorusDirectionCell.vue';
  import i18nDiffReportDetail from '@/components/chorus/diff-report-detail/i18nDiffReportDetail';

  const { t } = useI18n({ messages: i18nDiffReportDetail });
  const store = useChorusDiffReportDetailStore();
  const { createNotification, createRetryNotification } =
    useChorusNotification();
  const { createDialog } = useDialog();
  const { locations, isRestartLoading } = storeToRefs(store);

  async function restartDiffReport() {
    try {
      await store.restartDiffReport();

      createNotification({
        type: 'success',
        title: t('restartSuccessTitle'),
        content: t('restartSuccessContent'),
        duration: 4000,
      });
    } catch (error: unknown) {
      createRetryNotification({
        title: t('restartErrorTitle'),
        message: t('restartErrorContent'),
        error,
        positiveText: t('restartErrorAction'),
        positiveHandler: () => {
          restartDiffReport();
        },
      });
    }
  }

  function handleRestart() {
    createDialog({
      type: 'warning',
      iconName: IconName.BASE_REFRESH,
      title: t('restartConfirmTitle'),
      content: t('restartConfirmContent'),
      positiveText: t('restartConfirmAction'),
      negativeText: t('restartCancelAction'),
      positiveHandler: () => restartDiffReport(),
    });
  }
</script>

<template>
  <div
    class="diff-report-detail-header"
    v-if="locations[0] && locations[1]"
  >
    <div class="diff-report-detail-header__top">
      <div class="diff-report-detail-header__title">
        <h4>{{ t('detailTitle') }}</h4>
        <ChorusDirectionCell
          :from-text="locations[0].storage"
          :to-text="locations[1].storage"
          to-type="warning"
        >
          <template #from-extra>
            <CTag
              :bordered="false"
              round
              size="small"
              type="primary"
            >
              {{ locations[0].bucket }}
            </CTag>
          </template>
          <template #to-extra>
            <CTag
              :bordered="false"
              round
              size="small"
              type="primary"
            >
              {{ locations[1].bucket }}
            </CTag>
          </template>
        </ChorusDirectionCell>
      </div>

      <CButton
        type="warning"
        :loading="isRestartLoading"
        @click="handleRestart"
      >
        <template #icon>
          <CIcon
            :is-inline="true"
            :name="IconName.BASE_REFRESH"
          />
        </template>
        {{ t('restartAction') }}
      </CButton>
    </div>

    <p class="diff-report-detail-header__subtitle">
      {{ t('detailSubtitle') }}
    </p>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .diff-report-detail-header {
    &__top {
      display: flex;
      align-items: center;
      justify-content: space-between;
    }

    &__title {
      display: flex;
      align-items: center;
      gap: utils.unit(3);
    }
  }
</style>
