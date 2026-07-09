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
  import { useI18n } from 'vue-i18n';
  import {
    CResult,
    CSteps,
    CTile,
    DASHBOARD_NAV_META_INJECT_KEY,
    getDefaultNavMeta,
  } from '@clyso/clyso-ui-kit';
  import { inject, readonly, ref } from 'vue';
  import { useChorusAddDiffReportStore } from '@/stores/chorusAddDiffReportStore';
  import i18nAddDiffReport from '@/components/chorus/add-diff-report/i18nAddDiffReport';
  import AddDiffReportWizard from '@/components/chorus/add-diff-report/AddDiffReportWizard/AddDiffReportWizard.vue';

  const store = useChorusAddDiffReportStore();
  const { isLoading, hasError, hasEnoughStorages, currentStep, steps } =
    storeToRefs(store);

  const { t } = useI18n({ messages: i18nAddDiffReport });

  const navMeta = inject(
    DASHBOARD_NAV_META_INJECT_KEY,
    readonly(ref(getDefaultNavMeta())),
  );
</script>

<template>
  <CTile class="add-diff-report-tile">
    <template #title>
      {{ t('addDiffReportTitle') }}
    </template>
    <template
      #header
      v-if="!hasError && hasEnoughStorages && !isLoading"
    >
      {{ t('addDiffReportDescription') }}
    </template>

    <CResult
      v-if="hasError || (!hasEnoughStorages && !isLoading)"
      key="error"
      status="error"
      class="add-diff-report-tile__error"
      type="error"
      size="large"
      :max-width="600"
      @positive-click="store.initAddDiffReportPage"
    >
      <template #title>
        {{ t(hasError ? 'errorTitle' : 'notEnoughStoragesTitle') }}
      </template>

      <p>{{ t(hasError ? 'errorText' : 'notEnoughStoragesText') }}</p>

      <template #positive-text>
        {{ t('errorAction') }}
      </template>
    </CResult>

    <div
      v-else
      key="content"
      class="add-diff-report-tile__content"
    >
      <CSteps
        class="add-diff-report-tile__steps"
        :current="currentStep"
        :size="navMeta.isMobile ? 'small' : 'medium'"
        status="process"
        :steps="steps"
        :vertical="navMeta.isMobile ?? false"
      />

      <div class="add-diff-report-tile__form">
        <Transition
          name="opacity"
          mode="out-in"
        >
        </Transition>
      </div>

      <AddDiffReportWizard />
    </div>
  </CTile>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .add-diff-report-tile {
    &__steps {
      margin-bottom: utils.unit(10);
    }

    &__form {
      min-height: 300px;
      margin-bottom: utils.unit(10);
    }

    &__error {
      min-height: 500px;
    }
  }
</style>
