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
  import { IconName } from '@/utils/types/icon';

  const { t } = useI18n({
    messages: i18nRoutingPolicies,
  });

  const { routingPolicy } = defineProps<{
    routingPolicy: RoutingPolicy;
  }>();

  const {
    deleteRoutingPolicy: storeDeleteRoutingPolicy,
    setRoutingPolicyBlock: storeSetRoutingPolicyBlock,
  } = useChorusRoutingPoliciesStore();

  const { page, pagination, selectedRoutingPolicyIds } = storeToRefs(
    useChorusRoutingPoliciesStore(),
  );

  const { createNotification } = useChorusNotification();

  const { createDialog } = useDialog();

  const isDeleteLoading = ref(false);
  const isBlockLoading = ref(false);

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

      createNotification({
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
      createNotification({
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
      iconName: IconName.BASE_TRASH,
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

  async function setRoutingPolicyBlocked(setBlock: boolean) {
    isBlockLoading.value = true;

    try {
      await storeSetRoutingPolicyBlock(routingPolicy, setBlock);

      createNotification({
        type: 'success',
        title: setBlock ? t('blockSuccessTitle') : t('unblockSuccessTitle'),
        duration: 4000,
        content: () =>
          h('div', [
            setBlock ? t('blockSuccessContent') : t('unblockSuccessContent'),
            h(RoutingPoliciesShortList, {
              routingPolicies: [routingPolicy],
            }),
          ]),
      });
    } catch (error: unknown) {
      createNotification({
        type: 'error',
        title: setBlock ? t('blockErrorTitle') : t('unblockErrorTitle'),
        positiveText: setBlock
          ? t('blockErrorAction')
          : t('unblockErrorAction'),
        positiveHandler: () => {
          setRoutingPolicyBlocked(setBlock);
        },
        content: () =>
          h('div', [
            setBlock ? t('blockErrorContent') : t('unblockErrorContent'),
            h(RoutingPoliciesShortList, {
              routingPolicies: [[routingPolicy, error as string]],
            }),
          ]),
      });
    } finally {
      isBlockLoading.value = false;
    }
  }
</script>

<template>
  <div class="routing-policy-actions">
    <div class="routing-policy-actions__list">
      <div
        class="routing-policy-actions__item routing-policy-actions__item--block"
      >
        <CTooltip
          v-if="routingPolicy.isBlocked"
          :delay="1000"
        >
          <template #trigger>
            <CButton
              secondary
              size="tiny"
              type="success"
              :loading="isBlockLoading"
              @click="() => setRoutingPolicyBlocked(false)"
            >
              <template #icon>
                <CIcon
                  :is-inline="true"
                  :name="IconName.BASE_LOCK_OPEN"
                />
              </template>
            </CButton>
          </template>
          {{ t('actionUnblock') }}
        </CTooltip>

        <CTooltip
          v-else
          :delay="1000"
        >
          <template #trigger>
            <CButton
              secondary
              size="tiny"
              type="warning"
              :loading="isBlockLoading"
              @click="() => setRoutingPolicyBlocked(true)"
            >
              <template #icon>
                <CIcon
                  :is-inline="true"
                  :name="IconName.BASE_LOCK_CLOSED"
                />
              </template>
            </CButton>
          </template>
          {{ t('actionBlock') }}
        </CTooltip>
      </div>

      <div
        class="routing-policy-actions__item routing-policy-actions__item--delete"
      >
        <CTooltip :delay="1000">
          <template #trigger>
            <CButton
              secondary
              size="tiny"
              type="error"
              :loading="isDeleteLoading"
              :disabled="routingPolicy.isBlocked"
              @click="handleRoutingPolicyDelete"
            >
              <template #icon>
                <CIcon
                  :is-inline="true"
                  :name="IconName.BASE_TRASH"
                />
              </template>
            </CButton>
          </template>
          {{
            routingPolicy.isBlocked
              ? t('actionDeleteBlocked')
              : t('actionDelete')
          }}
        </CTooltip>
      </div>
    </div>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .routing-policy-actions__list {
    display: flex;
    gap: utils.unit(1);
  }
</style>
