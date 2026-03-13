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
  import { CDialog } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import { h, nextTick } from 'vue';
  import { useRouter } from 'vue-router';
  import i18nAddRoutingPolicy from '../i18nAddRoutingPolicy';
  import AddRoutingPolicySummary from '../AddRoutingPolicySummary/AddRoutingPolicySummary.vue';
  import { useChorusAddRoutingPolicyStore } from '@/stores/chorusAddRoutingPolicyStore';
  import { useChorusNotification } from '@/utils/composables/useChorusNotification';
  import { RouteName } from '@/utils/types/router';

  const { isConfirmDialogOpen } = storeToRefs(useChorusAddRoutingPolicyStore());
  const { addRoutingPolicy: callAddRoutingPolicyStore } =
    useChorusAddRoutingPolicyStore();
  const { addNotification } = useChorusNotification();

  const { t } = useI18n({
    messages: i18nAddRoutingPolicy,
  });

  const router = useRouter();

  async function addRoutingPolicy() {
    try {
      await callAddRoutingPolicyStore();

      addNotification({
        type: 'success',
        title: t('addRoutingPolicySuccessTitle'),
        content: t('addRoutingPolicySuccessContent'),
        duration: 4000,
      });

      isConfirmDialogOpen.value = false;

      // Is needed in order to navigate back to the overview table
      await nextTick();

      router.push({ name: RouteName.CHORUS_ROUTING_POLICIES });
    } catch (error: unknown) {
      addNotification({
        type: 'error',
        title: t('addRoutingPolicyErrorTitle'),
        positiveText: t('addErrorAction'),
        positiveHandler: () => {
          addRoutingPolicy();
        },
        content: () =>
          h('div', [
            t('addRoutingPolicyErrorContent'),
            ' -> ',
            error as string,
          ]),
      });
    }
  }
</script>

<template>
  <CDialog
    class="add-routing-policy-confirm-dialog"
    type="confirm"
    :is-shown="isConfirmDialogOpen"
    :width="600"
    :positive-handler="addRoutingPolicy"
    @update:is-shown="
      (value) => {
        isConfirmDialogOpen = value;
      }
    "
  >
    <template #title>
      {{ t('addRoutingPolicyConfirmTitle') }}
    </template>

    <div class="confirmation-details">
      <p class="confirmation-details__description">
        {{ t('confirmRoutingPolicyDescription') }}
      </p>

      <AddRoutingPolicySummary />
    </div>

    <template #positive-text>
      {{ t('confirmRoutingPolicyPositive') }}
    </template>
    <template #negative-text>
      {{ t('confirmRoutingPolicyNegative') }}
    </template>
  </CDialog>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .confirmation-details {
    &__description {
      margin-bottom: utils.unit(4);
    }
  }
</style>
