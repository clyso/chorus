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
  import { CSkeleton } from '@clyso/clyso-ui-kit';
  import { storeToRefs } from 'pinia';
  import { useI18n } from 'vue-i18n';
  import AddDiffReportBucket from '@/components/chorus/add-diff-report/AddDiffReportBucket/AddDiffReportBucket.vue';
  import AddDiffReportStorage from '@/components/chorus/add-diff-report/AddDiffReportStorage/AddDiffReportStorage.vue';
  import i18nAddDiffReport from '@/components/chorus/add-diff-report/i18nAddDiffReport';
  import { useChorusAddDiffReportStore } from '@/stores/chorusAddDiffReportStore';
  import { ErrorHelper } from '@/utils/helpers/ErrorHelper';

  const { storages, fromStorage, fromBucketName, validator, isLoading } =
    storeToRefs(useChorusAddDiffReportStore());

  const { t } = useI18n({
    messages: i18nAddDiffReport,
  });
</script>

<template>
  <div class="from-step">
    <div
      v-if="isLoading"
      key="loading"
      class="from-step__loading"
    >
      <CSkeleton
        type="text"
        width="30%"
        :margin-bottom="4"
      />

      <CSkeleton
        type="text"
        width="55%"
        :margin-bottom="8"
      />

      <div class="from-step__storages-list">
        <CSkeleton
          v-for="(_, index) in Array(4)"
          :key="index"
          :margin-bottom="12"
          :border-radius="8"
          :height="106"
        />
      </div>

      <div class="from-step__bucket">
        <CSkeleton
          type="text"
          width="25%"
          :margin-bottom="4"
        />

        <CSkeleton
          type="text"
          width="50%"
          :margin-bottom="8"
        />

        <CSkeleton
          :border-radius="8"
          :height="40"
          width="400px"
        />
      </div>
    </div>

    <div
      v-else
      key="content"
      class="from-step__content"
    >
      <div class="from-step__storages-list">
        <AddDiffReportStorage
          v-model="fromStorage"
          :storages="storages"
          :title="t('fromStorageTitle')"
          :description="t('fromStorageDescription')"
          :has-error="validator.fromStorage.$error"
          :error-message="
            ErrorHelper.getValidationErrorMessage(validator.fromStorage)
          "
        />
      </div>
      <div class="from-step__bucket">
        <AddDiffReportBucket
          v-model="fromBucketName"
          field-id="from-bucket-input"
          :title="t('fromBucketTitle')"
          :description="t('fromBucketDescription')"
          :has-error="validator.fromBucketName.$error"
          :error-message="
            ErrorHelper.getValidationErrorMessage(validator.fromBucketName)
          "
          @blur="validator.fromBucketName.$touch()"
        />
      </div>
    </div>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .from-step {
    &__storages-list {
      display: grid;
      gap: utils.unit(3);
      grid-auto-flow: column;
    }

    &__bucket {
      margin-top: utils.unit(6);
    }
  }
</style>
