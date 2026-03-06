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
  import { useRouter } from 'vue-router';
  import RoutingPoliciesShortList from '../RoutingPoliciesShortList/RoutingPoliciesShortList.vue';
  import i18nRoutingPolicies from '../i18nRoutingPolicies';
  import { IconName } from '@/utils/types/icon';
  import { useChorusRoutingPoliciesStore } from '@/stores/chorusRoutingPoliciesStore';
  import { useChorusNotification } from '@/utils/composables/useChorusNotification';
  import type { RoutingPolicy } from '@/utils/types/chorus';
  import { RouteName } from '@/utils/types/router';

  const { createDialog } = useDialog();

  const {
    selectedRoutingPolicies,
    isAnyRoutingPolicySelected,
    isDeleteSelectedProcessing,
  } = storeToRefs(useChorusRoutingPoliciesStore());

  const {
    deleteRoutingPolicies: deleteRoutingPoliciesStore,
    setRoutingPoliciesBlock,
  } = useChorusRoutingPoliciesStore();

  const { addNotification } = useChorusNotification();

  const { t } = useI18n({
    messages: i18nRoutingPolicies,
  });

  const router = useRouter();

  const selectedRoutingPoliciesForBlock = computed(() =>
    selectedRoutingPolicies.value.filter(
      (routingPolicy) => !routingPolicy.isBlocked,
    ),
  );
  const selectedRoutingPoliciesForUnblock = computed(() =>
    selectedRoutingPolicies.value.filter(
      (routingPolicy) => routingPolicy.isBlocked,
    ),
  );
  const isDeleteDisabled = computed(() => !isAnyRoutingPolicySelected.value);
  const isBlockDisabled = computed(
    () =>
      !isAnyRoutingPolicySelected.value ||
      selectedRoutingPoliciesForBlock.value.length <= 0,
  );
  const isUnblockDisabled = computed(
    () =>
      !isAnyRoutingPolicySelected.value ||
      selectedRoutingPoliciesForUnblock.value.length <= 0,
  );

  async function deleteRoutingPolicies(routingPolicies: RoutingPolicy[]) {
    const { successList, errorList } =
      await deleteRoutingPoliciesStore(routingPolicies);

    if (successList.length > 0) {
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
    }

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

  async function updateRoutingPoliciesBlocked(
    routingPolicies: RoutingPolicy[],
    setBlock: boolean,
  ) {
    const { successList, errorList } = await setRoutingPoliciesBlock(
      routingPolicies,
      setBlock,
    );

    if (successList.length > 0) {
      addNotification({
        type: 'success',
        title: setBlock ? t('blockSuccessTitle') : t('unblockSuccessTitle'),
        duration: 4000,
        content: () =>
          h('div', [
            setBlock
              ? t('blockSelectedSuccessContent', { total: successList.length })
              : t('unblockSelectedSuccessContent', {
                  total: successList.length,
                }),
            h(RoutingPoliciesShortList, {
              routingPolicies: successList,
            }),
          ]),
      });
    }

    if (errorList.length > 0) {
      addNotification({
        type: 'error',
        title: setBlock ? t('blockErrorTitle') : t('unblockErrorTitle'),
        positiveText: setBlock
          ? t('blockErrorAction')
          : t('unblockErrorAction'),
        positiveHandler: () => {
          updateRoutingPoliciesBlocked(
            errorList.map(([routingPolicy]) => routingPolicy),
            setBlock,
          );
        },
        content: () =>
          h('div', [
            setBlock
              ? t('blockSelectedErrorContent', { total: errorList.length })
              : t('unblockSelectedErrorContent', { total: errorList.length }),
            h(RoutingPoliciesShortList, {
              routingPolicies: errorList,
            }),
          ]),
      });
    }
  }

  function openSetBlockConfirmation(setBlock: boolean) {
    const routingPolicies = setBlock
      ? selectedRoutingPoliciesForBlock.value
      : selectedRoutingPoliciesForUnblock.value;

    const dialogContent = setBlock
      ? {
          type: 'warning' as const,
          icon: IconName.BASE_LOCK_CLOSED,
          title: t('actionSelectedBlockTitle'),
          content: 'actionSelectedBlockContent',
          question: t('actionSelectedBlockQuestion'),
          action: t('actionBlock'),
        }
      : {
          type: 'success' as const,
          icon: IconName.BASE_LOCK_OPEN,
          title: t('actionSelectedUnblockTitle'),
          content: 'actionSelectedUnblockContent',
          question: t('actionSelectedUnblockQuestion'),
          action: t('actionUnblock'),
        };

    createDialog({
      type: dialogContent.type,
      iconName: dialogContent.icon,
      title: dialogContent.title,
      content: () => [
        h(
          'div',
          { style: 'margin-bottom: 8px' },
          t(dialogContent.content, { total: routingPolicies.length }),
        ),
        h(RoutingPoliciesShortList, {
          routingPolicies,
          size: 'medium',
          style: 'margin-bottom: 8px',
        }),
        dialogContent.question,
      ],
      positiveText: dialogContent.action,
      negativeText: t('cancel'),
      positiveHandler: () =>
        updateRoutingPoliciesBlocked(routingPolicies, setBlock),
    });
  }

  function goToAddRoutingPolicy() {
    router.push({ name: RouteName.CHORUS_ADD_ROUTING_POLICY });
  }
</script>

<template>
  <div class="routing-policies-list-actions">
    <div class="routing-policies-list-actions__creation">
      <CButton
        size="medium"
        type="primary"
        ghost
        class="add-routing-policy-button"
        @click="goToAddRoutingPolicy"
        tag="div"
      >
        <template #icon>
          <CIcon
            :is-inline="true"
            :name="IconName.BASE_ADD"
          />
        </template>
        {{ t('actionAddRoutingPolicy') }}
      </CButton>
    </div>

    <div class="routing-policies-list-actions__selection-actions">
      <CTooltip :delay="1000">
        <template #trigger>
          <CBadge
            :offset="[-4, 0]"
            :value="selectedRoutingPoliciesForUnblock.length"
            :max="100"
          >
            <CButton
              secondary
              :disabled="isUnblockDisabled"
              size="medium"
              type="success"
              @click="openSetBlockConfirmation(false)"
            >
              <template #icon>
                <CIcon
                  :is-inline="true"
                  :name="IconName.BASE_LOCK_OPEN"
                />
              </template>
            </CButton>
          </CBadge>
        </template>
        {{ t('actionUnblock') }}
      </CTooltip>

      <CTooltip :delay="1000">
        <template #trigger>
          <CBadge
            :offset="[-4, 0]"
            :value="selectedRoutingPoliciesForBlock.length"
            :max="100"
          >
            <CButton
              secondary
              :disabled="isBlockDisabled"
              size="medium"
              type="warning"
              @click="openSetBlockConfirmation(true)"
            >
              <template #icon>
                <CIcon
                  :is-inline="true"
                  :name="IconName.BASE_LOCK_CLOSED"
                />
              </template>
            </CButton>
          </CBadge>
        </template>
        {{ t('actionBlock') }}
      </CTooltip>

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

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .routing-policies-list-actions {
    display: flex;
    flex-direction: row-reverse;
    justify-content: space-between;
    gap: utils.unit(2);

    .routing-policies-list-actions__selection-actions {
      display: flex;
      gap: utils.unit(2);
    }
  }
</style>
