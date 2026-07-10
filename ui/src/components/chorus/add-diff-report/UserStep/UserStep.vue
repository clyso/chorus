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
  import { useChorusAddDiffReportStore } from '@/stores/chorusAddDiffReportStore';
  import ChorusUserCardList from '@/components/chorus/common/ChorusUserCardList/ChorusUserCardList.vue';
  import i18nAddDiffReport from '@/components/chorus/add-diff-report/i18nAddDiffReport';
  import { ErrorHelper } from '@/utils/helpers/ErrorHelper';

  const { selectedUser, users, validator } = storeToRefs(
    useChorusAddDiffReportStore(),
  );

  const { t } = useI18n({ messages: i18nAddDiffReport });
</script>

<template>
  <div class="add-diff-report-user-selection">
    <h5 class="add-diff-report-user-selection__title">
      {{ t('userTitle') }}
    </h5>
    <p class="add-diff-report-user-selection__description">
      {{ t('userDescription') }}
    </p>

    <ChorusUserCardList
      v-model="selectedUser"
      :users="users"
      :empty-message="t('noUsersText')"
      :empty-message-title="t('noUsersTitle')"
    />

    <span
      v-if="validator.selectedUser.$error"
      class="add-diff-report-user-selection__error"
    >
      {{ ErrorHelper.getValidationErrorMessage(validator.selectedUser) }}
    </span>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .add-diff-report-user-selection {
    &__title {
      margin-bottom: utils.unit(2);
    }

    &__description {
      margin-bottom: utils.unit(10);
    }

    &__error {
      color: var(--error-color);
      font-size: 12px;
    }
  }
</style>
