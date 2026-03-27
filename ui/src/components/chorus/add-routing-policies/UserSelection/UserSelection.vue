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
  import { computed, unref } from 'vue';
  import i18nAddRoutingPolicy from '../i18nAddRoutingPolicy';
  import ChorusUserCardList from '../../common/ChorusUserCardList/ChorusUserCardList.vue';
  import { useChorusAddRoutingPolicyStore } from '@/stores/chorusAddRoutingPolicyStore';

  const { selectedUser, users, validator } = storeToRefs(
    useChorusAddRoutingPolicyStore(),
  );

  const { t } = useI18n({
    messages: i18nAddRoutingPolicy,
  });

  const userSelectionErrorMessage = computed(() => {
    const errors = validator.value.selectedUser.$errors;

    if (!errors || errors.length === 0) return '';

    return t(unref(errors[0]?.$message) || 'unknownValidationError');
  });
</script>

<template>
  <div class="user-selection">
    <div class="user-selection__header">
      <h5 class="user-selection__title">{{ t('userSelectionTitle') }}</h5>
      <p class="user-selection__description">
        {{ t('userSelectionDescription') }}
      </p>
    </div>

    <div class="user-selection__content">
      <div class="user-selection__selector">
        <ChorusUserCardList
          v-model="selectedUser"
          :users="users"
        />
      </div>
      <span
        v-if="validator.selectedUser.$error"
        class="user-selection__error-text"
      >
        {{ userSelectionErrorMessage }}
      </span>
    </div>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .user-selection {
    &__title,
    &__description {
      margin-bottom: utils.unit(1);
    }

    &__error-text {
      color: var(--error-color);
      font-size: 12px;
    }
  }
</style>
