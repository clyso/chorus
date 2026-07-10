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
  import AddDiffReportBucket from '@/components/chorus/add-diff-report/AddDiffReportBucket/AddDiffReportBucket.vue';
  import AddDiffReportStorage from '@/components/chorus/add-diff-report/AddDiffReportStorage/AddDiffReportStorage.vue';
  import i18nAddDiffReport from '@/components/chorus/add-diff-report/i18nAddDiffReport';
  import { useChorusAddDiffReportStore } from '@/stores/chorusAddDiffReportStore';
  import { ErrorHelper } from '@/utils/helpers/ErrorHelper';

  const { storages, fromStorage, toStorage, toBucketName, validator } =
    storeToRefs(useChorusAddDiffReportStore());

  const { t } = useI18n({
    messages: i18nAddDiffReport,
  });
</script>

<template>
  <div class="to-step">
    <div class="to-step__storages-list">
      <AddDiffReportStorage
        v-model="toStorage"
        :storages="storages"
        :title="t('toStorageTitle')"
        :description="t('toStorageDescription')"
        :has-error="validator.toStorage.$error"
        :error-message="
          ErrorHelper.getValidationErrorMessage(validator.toStorage)
        "
        :disabled-storage-name="fromStorage?.name"
      />
    </div>
    <div class="to-step__bucket">
      <AddDiffReportBucket
        v-model="toBucketName"
        field-id="to-bucket-input"
        :title="t('toBucketTitle')"
        :description="t('toBucketDescription')"
        :has-error="validator.toBucketName.$error"
        :error-message="
          ErrorHelper.getValidationErrorMessage(validator.toBucketName)
        "
        @blur="validator.toBucketName.$touch()"
      />
    </div>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .to-step {
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
