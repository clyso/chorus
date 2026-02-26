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
  import {
    CBadge,
    CButton,
    CTooltip,
    CIcon,
    useDialog,
  } from '@clyso/clyso-ui-kit';
  import { storeToRefs } from 'pinia';
  import { computed, h } from 'vue';
  import { useI18n } from 'vue-i18n';
  import RoutingPoliciesShortList from '../RoutingPoliciesShortList/RoutingPoliciesShortList.vue';
  import i18nRoutingPolicies from '../i18nRoutingPolicies';
  import { IconName } from '@/utils/types/icon';
  import { useChorusRoutingPoliciesStore } from '@/stores/chorusRoutingPoliciesStore';
  import { useChorusNotification } from '@/utils/composables/useChorusNotification';
  import type { RoutingPolicy } from '@/utils/types/chorus';

  const { createDialog } = useDialog();

  const {
    selectedRoutingPolicies,
    isAnyRoutingPolicySelected,
    isDeleteSelectedProcessing,
  } = storeToRefs(useChorusRoutingPoliciesStore());
  const { deleteRoutingPolicies: deleteRoutingPoliciesStore } =
    useChorusRoutingPoliciesStore();
  const { addNotification } = useChorusNotification();

  const { t } = useI18n({
    messages: i18nRoutingPolicies,
  });

  const isDeleteDisabled = computed(() => !isAnyRoutingPolicySelected.value);

  async function deleteRoutingPolicies(routingPolicies: RoutingPolicy[]) {
    const { successList, errorList } =
      await deleteRoutingPoliciesStore(routingPolicies);

    addNotification({
      type: 'success',
      title: t('deleteSuccessTitle'),
      duration: 4000,
      content: () =>
        h('div', [
          t('deleteSelectedSuccessContent', { total: successList.length }),
          h(RoutingPoliciesShortList, {
            routingPolicies: successList,
          }),
        ]),
    });

    if (errorList.length > 0) {
      addNotification({
        type: 'error',
        title: t('deleteErrorTitle'),
        positiveText: t('deleteErrorAction'),
        positiveHandler: () => {
          deleteRoutingPolicies(
            errorList.map(([routingPolicy]) => routingPolicy),
          );
        },
        content: () =>
          h('div', [
            t('deleteSelectedErrorContent', { total: errorList.length }),
            h(RoutingPoliciesShortList, {
              routingPolicies: errorList,
            }),
          ]),
      });
    }
  }

  function openDeleteConfirmation() {
    const routingPolicies = selectedRoutingPolicies.value;

    createDialog({
      type: 'error',
      iconName: IconName.BASE_TRASH,
      title: t('actionSelectedDeleteTitle'),
      content: () => [
        h(
          'div',
          { style: 'margin-bottom: 8px' },
          t('actionSelectedDeleteContent', { total: routingPolicies.length }),
        ),
        h(RoutingPoliciesShortList, {
          routingPolicies,
          size: 'medium',
          style: 'margin-bottom: 8px',
        }),
        t('actionSelectedDeleteQuestion'),
      ],
      positiveText: t('actionDelete'),
      negativeText: t('cancel'),
      positiveHandler: () => deleteRoutingPolicies(routingPolicies),
    });
  }
</script>

<template>
  <div class="routing-policies-list-actions">
    <div class="routing-policies-list-actions__selection-actions">
      <CTooltip :delay="1000">
        <template #trigger>
          <CBadge
            :offset="[-4, 0]"
            :value="selectedRoutingPolicies.length"
            :max="100"
          >
            <CButton
              secondary
              :disabled="isDeleteDisabled"
              :loading="isDeleteSelectedProcessing"
              size="medium"
              type="error"
              @click="openDeleteConfirmation"
            >
              <template #icon>
                <CIcon
                  :is-inline="true"
                  :name="IconName.BASE_TRASH"
                />
              </template>
            </CButton>
          </CBadge>
        </template>
        {{ t('actionDelete') }}
      </CTooltip>
    </div>
  </div>
</template>
