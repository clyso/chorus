<!--
  - Copyright © 2025 Clyso GmbH
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
  import { CButton, CIcon } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import { computed } from 'vue';
  import { storeToRefs } from 'pinia';
  import i18nAddReplication from '@/components/chorus/add-replication/i18nAddReplication';
  import { IconName } from '@/utils/types/icon';
  import { useChorusAddReplicationStore } from '@/stores/chorusAddReplicationStore';
  import { AddReplicationStepName } from '@/utils/types/chorus';

  const { t } = useI18n({
    messages: i18nAddReplication,
  });

  const {
    currentStep,
    stepsCount,
    isLoading,
    validator,
    isConfirmDialogOpen,
    isSubmitting,
    selectedFromStorage,
    selectedToStorage,
    selectedUser,
  } = storeToRefs(useChorusAddReplicationStore());

  const isLastStep = computed<boolean>(
    () => currentStep.value === stepsCount.value,
  );
  const isFirstStep = computed<boolean>(() => currentStep.value === 1);

  const isNextDisabled = computed<boolean>(() => {
    if (isLoading.value || isLastStep.value) {
      return true;
    }

    if (currentStep.value === AddReplicationStepName.FROM_STORAGE) {
      return selectedFromStorage.value === null;
    }

    if (currentStep.value === AddReplicationStepName.TO_STORAGE) {
      return selectedToStorage.value === null;
    }

    if (currentStep.value === AddReplicationStepName.USER) {
      return selectedUser.value === null;
    }

    return false;
  });

  function goNext() {
    if (isNextDisabled.value) {
      return;
    }

    currentStep.value += 1;
  }

  function goBack() {
    if (isFirstStep.value) {
      return;
    }

    currentStep.value -= 1;
  }

  function handleSubmit() {
    validator.value.$touch();

    if (validator.value.$error) {
      return;
    }

    isConfirmDialogOpen.value = true;
  }
</script>

<template>
  <div class="add-replication-actions">
    <CButton
      secondary
      :disabled="isFirstStep || isSubmitting"
      size="large"
      class="add-replication-actions__back"
      :loading="false"
      @click="goBack"
    >
      <template #icon>
        <CIcon
          :is-inline="true"
          :name="IconName.BASE_ARROW_BACK"
        />
      </template>
    </CButton>

    <CButton
      v-if="!isLastStep"
      type="primary"
      ghost
      size="large"
      :disabled="isNextDisabled"
      class="add-replication-actions__next"
      @click="goNext"
    >
      <template #icon>
        <CIcon
          :is-inline="true"
          :name="IconName.BASE_ARROW_FORWARD"
        />
      </template>
      {{ t('nextAction') }}
    </CButton>
    <CButton
      v-else
      type="primary"
      :disabled="isSubmitting"
      :loading="isSubmitting"
      size="large"
      class="add-replication-actions__submit"
      @click="handleSubmit"
    >
      {{ t('addReplicationAction') }}
    </CButton>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .add-replication-actions {
    display: flex;
    gap: utils.unit(3);
  }
</style>
