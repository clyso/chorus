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
  import { CFormField, CInput } from '@clyso/clyso-ui-kit';
  import i18nAddDiffReport from '@/components/chorus/add-diff-report/i18nAddDiffReport';

  defineProps<{
    modelValue: string;
    fieldId: string;
    title: string;
    description: string;
    hasError: boolean;
    errorMessage: string;
  }>();

  const emit = defineEmits<{
    (e: 'update:modelValue', value: string): void;
    (e: 'blur'): void;
  }>();

  const { t } = useI18n({ messages: i18nAddDiffReport });
</script>

<template>
  <div class="add-diff-report-bucket">
    <h5 class="add-diff-report-bucket__title">{{ title }}</h5>
    <p class="add-diff-report-bucket__description">{{ description }}</p>

    <CFormField
      :field-id="fieldId"
      class="add-diff-report-bucket__field"
      :has-error="hasError"
    >
      <template #default="{ hasError: fieldHasError, fieldId: fId }">
        <CInput
          :id="fId"
          :value="modelValue"
          :placeholder="t('bucketPlaceholder')"
          :has-error="fieldHasError"
          @update:value="emit('update:modelValue', $event)"
          @blur="emit('blur')"
        />
      </template>

      <template #errors>
        <template v-if="hasError">
          {{ errorMessage }}
        </template>
      </template>
    </CFormField>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .add-diff-report-bucket {
    &__title,
    &__description {
      margin-bottom: utils.unit(1);
    }

    &__field {
      max-width: 400px;
    }
  }
</style>
