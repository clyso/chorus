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
  import { storeToRefs } from 'pinia';
  import { CSwitch } from '@clyso/clyso-ui-kit';
  import i18nAddRoutingPolicy from '../i18nAddRoutingPolicy';
  import { useChorusAddRoutingPolicyStore } from '@/stores/chorusAddRoutingPolicyStore';

  const { isBlocked } = storeToRefs(useChorusAddRoutingPolicyStore());

  const { t } = useI18n({
    messages: i18nAddRoutingPolicy,
  });

  function setStatus(value: boolean) {
    isBlocked.value = value;
  }
</script>

<template>
  <div class="status-selection">
    <div class="status-selection__header">
      <h5 class="status-selection__title">{{ t('statusSelectionTitle') }}</h5>
      <p class="status-selection__description">
        {{ t('statusSelectionDescription') }}
      </p>
    </div>

    <div class="status-selection__content">
      <div
        class="status-selection__switcher"
        tabindex="0"
        @click="setStatus(!isBlocked)"
        @keydown.enter.prevent="setStatus(!isBlocked)"
        @keydown.space.prevent="setStatus(!isBlocked)"
      >
        <CSwitch
          :value="isBlocked"
          size="small"
          tabindex="-1"
        />
        <span class="status-selection__label">
          {{
            isBlocked
              ? t('statusSelectionBlocked')
              : t('statusSelectionAllowed')
          }}
        </span>
      </div>
    </div>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .status-selection {
    &__title,
    &__description {
      margin-bottom: utils.unit(1);
    }

    &__switcher {
      display: flex;
      gap: utils.unit(2);
      cursor: pointer;
    }
  }
</style>
