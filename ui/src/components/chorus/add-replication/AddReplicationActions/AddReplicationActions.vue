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
  import { computed } from 'vue';
  import { storeToRefs } from 'pinia';
  import i18nAddReplication from '@/components/chorus/add-replication/i18nAddReplication';
  import { useChorusAddReplicationStore } from '@/stores/chorusAddReplicationStore';
  import { AddReplicationStepName } from '@/utils/types/chorus';
  import ChorusWizard from '@/components/chorus/common/ChorusWizard/ChorusWizard.vue';

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

  const isNextDisabled = computed<boolean>(() => {
    if (isLoading.value || currentStep.value === stepsCount.value) {
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

  function handleSubmit() {
    validator.value.$touch();

    if (validator.value.$error) {
      return;
    }

    isConfirmDialogOpen.value = true;
  }
</script>

<template>
  <ChorusWizard
    v-model:current-step="currentStep"
    :steps-count="stepsCount"
    :is-submitting="isSubmitting"
    :is-next-disabled="isNextDisabled"
    :submit-label="t('addReplicationAction')"
    :next-label="t('nextAction')"
    @submit="handleSubmit"
  />
</template>
