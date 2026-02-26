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
  import { CButton, CIcon, CTooltip, useDialog } from '@clyso/clyso-ui-kit';
  import { h, ref } from 'vue';
  import { storeToRefs } from 'pinia';
  import i18nRoutingPolicies from '../i18nRoutingPolicies';
  import RoutingPoliciesShortList from '../RoutingPoliciesShortList/RoutingPoliciesShortList.vue';
  import type { RoutingPolicy } from '@/utils/types/chorus';
  import { useChorusRoutingPoliciesStore } from '@/stores/chorusRoutingPoliciesStore';
  import { useChorusNotification } from '@/utils/composables/useChorusNotification';

  const { t } = useI18n({
    messages: i18nRoutingPolicies,
  });

  const { routingPolicy } = defineProps<{
    routingPolicy: RoutingPolicy;
  }>();

  const { deleteRoutingPolicy: storeDeleteRoutingPolicy } =
    useChorusRoutingPoliciesStore();

  const { page, pagination, selectedRoutingPolicyIds } = storeToRefs(
    useChorusRoutingPoliciesStore(),
  );

  const { addNotification } = useChorusNotification();

  const { createDialog } = useDialog();

  const isDeleteLoading = ref(false);

  async function deleteRoutingPolicy() {
    isDeleteLoading.value = true;

    try {
      await storeDeleteRoutingPolicy(routingPolicy);

      const { pageCount } = pagination.value;

      if (pageCount !== undefined && page.value > pageCount) {
        page.value = pagination.value.pageCount || 1;
      }

      selectedRoutingPolicyIds.value = selectedRoutingPolicyIds.value.filter(
        (selectedId) => selectedId !== routingPolicy.id,
      );

      addNotification({
        type: 'success',
        title: t('deleteSuccessTitle'),
        duration: 4000,
        content: () =>
          h('div', [
            t('deleteSuccessContent'),
            h(RoutingPoliciesShortList, {
              routingPolicies: [routingPolicy],
            }),
          ]),
      });
    } catch (error: unknown) {
      addNotification({
        type: 'error',
        title: t('deleteErrorTitle'),
        positiveText: t('deleteErrorAction'),
        positiveHandler: () => {
          deleteRoutingPolicy();
        },
        content: () =>
          h('div', [
            t('deleteErrorContent'),
            h(RoutingPoliciesShortList, {
              routingPolicies: [[routingPolicy, error as string]],
            }),
          ]),
      });
    } finally {
      isDeleteLoading.value = false;
    }
  }

  function handleRoutingPolicyDelete() {
    createDialog({
      type: 'error',
      iconName: 'base-trash',
      title: t('routingPolicyDeletionConfirmTitle'),
      content: () => [
        h(
          'div',
          { style: 'margin-bottom: 8px' },
          t('routingPolicyDeletionConfirmContent'),
        ),
        h(RoutingPoliciesShortList, {
          routingPolicies: [routingPolicy],
          size: 'medium',
          style: 'margin-bottom: 8px',
        }),
        t('routingPolicyDeletionConfirmQuestion'),
      ],
      positiveText: t('routingPolicyDeletionConfirmAction'),
      negativeText: t('routingPolicyDeletionCancelAction'),
      positiveHandler: () => deleteRoutingPolicy(),
    });
  }
</script>

<template>
  <div class="routing-policy-actions">
    <div class="routing-policy-actions__list">
      <div
        class="routing-policy-action__item routing-policy-action__item--delete"
      >
        <CTooltip :delay="1000">
          <template #trigger>
            <CButton
              secondary
              size="tiny"
              type="error"
              :loading="isDeleteLoading"
              @click="handleRoutingPolicyDelete"
            >
              <template #icon>
                <CIcon
                  :is-inline="true"
                  name="base-trash"
                />
              </template>
            </CButton>
          </template>
          {{ t('actionDelete') }}
        </CTooltip>
      </div>
    </div>
  </div>
</template>
