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
    CButton,
    CIcon,
    CTooltip,
    type NotificationConfig,
    useDialog,
    useNotification,
  } from '@clyso/clyso-ui-kit';
  import { computed, h, type Ref, ref } from 'vue';
  import { storeToRefs } from 'pinia';
  import { useI18n } from 'vue-i18n';
  import type {
    ChorusReplication,
    ChorusUserReplication,
  } from '@/utils/types/chorus';
  import type { AddId } from '@/utils/types/helper';
  import { useChorusReplicationsStore } from '@/stores/chorusReplicationsStore';
  import { IconName } from '@/utils/types/icon';
  import ReplicationsShortList from '@/components/chorus/replications/ReplicationsShortList/ReplicationsShortList.vue';
  import i18nReplications from '@/components/chorus/replications/i18nReplications';

  const { t } = useI18n({
    messages: i18nReplications,
  });

  const props = defineProps<{
    replication: AddId<ChorusReplication>;
  }>();

  const isPauseResumeLoading = ref(false);
  const isDeleteLoading = ref(false);

  const {
    setReplicationPaused,
    deleteReplication: callDeleteReplication,
    setUserReplicationToDelete,
  } = useChorusReplicationsStore();

  const {
    page,
    pagination,
    userReplications,
    userReplicationToDelete,
    selectedReplicationIds,
  } = storeToRefs(useChorusReplicationsStore());

  const { createNotification, removeNotification } = useNotification();

  const replicationNotificationId: Ref<string | null> = ref(null);

  function clearReplicationNotification() {
    if (!replicationNotificationId.value) {
      return;
    }

    const notificationId = replicationNotificationId.value;

    setTimeout(() => removeNotification(notificationId));
    replicationNotificationId.value = null;
  }

  function createReplicationNotification(config: NotificationConfig) {
    clearReplicationNotification();

    replicationNotificationId.value = createNotification(config).value.id;
  }

  async function pauseReplication() {
    isPauseResumeLoading.value = true;

    try {
      await setReplicationPaused(props.replication, true);

      createReplicationNotification({
        type: 'success',
        title: t('pauseSuccessTitle'),
        duration: 4000,
        content: () =>
          h('div', [
            t('pauseSuccessContent'),
            h(ReplicationsShortList, {
              replications: [props.replication],
            }),
          ]),
      });
    } catch {
      createReplicationNotification({
        type: 'error',
        title: t('pauseErrorTitle'),
        positiveText: t('pauseErrorAction'),
        positiveHandler: () => {
          clearReplicationNotification();
          pauseReplication();
        },
        content: () =>
          h('div', [
            t('pauseErrorContent'),
            h(ReplicationsShortList, {
              replications: [props.replication],
            }),
          ]),
      });
    } finally {
      isPauseResumeLoading.value = false;
    }
  }

  async function resumeReplication() {
    isPauseResumeLoading.value = true;

    try {
      await setReplicationPaused(props.replication, false);

      createReplicationNotification({
        type: 'success',
        title: t('resumeSuccessTitle'),
        duration: 4000,
        content: () =>
          h('div', [
            t('resumeSuccessContent'),
            h(ReplicationsShortList, {
              replications: [props.replication],
            }),
          ]),
      });
    } catch {
      createReplicationNotification({
        type: 'error',
        title: t('resumeErrorTitle'),
        positiveText: t('resumeErrorAction'),
        positiveHandler: () => {
          clearReplicationNotification();
          resumeReplication();
        },
        content: () =>
          h('div', [
            t('resumeErrorContent'),
            h(ReplicationsShortList, {
              replications: [props.replication],
            }),
          ]),
      });
    } finally {
      isPauseResumeLoading.value = false;
    }
  }

  async function deleteReplication() {
    isDeleteLoading.value = true;

    try {
      await callDeleteReplication(props.replication);

      const { pageCount } = pagination.value;

      if (pageCount !== undefined && page.value > pageCount) {
        page.value = pagination.value.pageCount || 1;
      }

      selectedReplicationIds.value = selectedReplicationIds.value.filter(
        (selectedId) => selectedId !== props.replication.id,
      );

      createReplicationNotification({
        type: 'success',
        title: t('deleteSuccessTitle'),
        duration: 4000,
        content: () =>
          h('div', [
            t('deleteSuccessContent'),
            h(ReplicationsShortList, {
              replications: [props.replication],
            }),
          ]),
      });
    } catch {
      createReplicationNotification({
        type: 'error',
        title: t('deleteErrorTitle'),
        positiveText: t('deleteErrorAction'),
        positiveHandler: () => {
          clearReplicationNotification();
          deleteReplication();
        },
        content: () =>
          h('div', [
            t('deleteErrorContent'),
            h(ReplicationsShortList, {
              replications: [props.replication],
            }),
          ]),
      });
    } finally {
      isDeleteLoading.value = false;
    }
  }

  const { createDialog } = useDialog();

  function handleBucketReplicationDelete() {
    createDialog({
      type: 'error',
      iconName: 'base-trash',
      title: t('bucketDeletionConfirmTitle'),
      content: () => [
        h(
          'div',
          { style: 'margin-bottom: 8px' },
          t('bucketDeletionConfirmContent'),
        ),
        h(ReplicationsShortList, {
          replications: [props.replication],
          size: 'medium',
          style: 'margin-bottom: 8px',
        }),
        t('bucketDeletionConfirmQuestion'),
      ],
      positiveText: t('bucketDeletionConfirmAction'),
      negativeText: t('bucketDeletionConfirmCancel'),
      positiveHandler: () => deleteReplication(),
    });
  }

  const userReplication = computed<ChorusUserReplication | null>(
    () =>
      userReplications.value.find(
        (userReplicationItem) =>
          props.replication.user === userReplicationItem.user &&
          props.replication.to === userReplicationItem.to &&
          props.replication.from === userReplicationItem.from,
      ) ?? null,
  );

  function handleUserReplicationDelete() {
    setUserReplicationToDelete(userReplication.value);
  }
</script>

<template>
  <div class="replication-actions-cell">
    <div class="actions-list">
      <!--      TODO: uncomment when Replication Details page is implemented-->
      <!--      <div class="actions-list__item actions-list__item&#45;&#45;details">-->
      <!--        <CTooltip :delay="1000">-->
      <!--          <template #trigger>-->
      <!--            <RouterLink to="/">-->
      <!--              <CButton-->
      <!--                secondary-->
      <!--                size="tiny"-->
      <!--                tag="div"-->
      <!--              >-->
      <!--                <template #icon>-->
      <!--                  <CIcon-->
      <!--                    :is-inline="true"-->
      <!--                    name="eye"-->
      <!--                  />-->
      <!--                </template>-->
      <!--              </CButton>-->
      <!--            </RouterLink>-->
      <!--          </template>-->

      <!--          {{ t('actionViewDetails') }}-->
      <!--        </CTooltip>-->
      <!--      </div>-->

      <div class="actions-list__item actions-list__item--pause-resume">
        <CTooltip
          v-if="replication.isPaused"
          key="resume"
          :delay="1000"
        >
          <template #trigger>
            <CButton
              secondary
              size="tiny"
              type="info"
              :loading="isPauseResumeLoading"
              @click="resumeReplication"
            >
              <template #icon>
                <CIcon
                  :is-inline="true"
                  :name="IconName.BASE_PLAY"
                />
              </template>
            </CButton>
          </template>

          {{ t('actionResume') }}
        </CTooltip>

        <CTooltip
          v-else
          key="pause"
          :delay="1000"
        >
          <template #trigger>
            <CButton
              secondary
              size="tiny"
              type="warning"
              :loading="isPauseResumeLoading"
              @click="pauseReplication"
            >
              <template #icon>
                <CIcon
                  :is-inline="true"
                  :name="IconName.BASE_PAUSE"
                />
              </template>
            </CButton>
          </template>

          {{ t('actionPause') }}
        </CTooltip>
      </div>

      <CTooltip :delay="1000">
        <template #trigger>
          <CButton
            secondary
            size="tiny"
            type="error"
            :loading="isDeleteLoading"
            @click="handleBucketReplicationDelete"
          >
            <template #icon>
              <CIcon
                :is-inline="true"
                :name="IconName.BASE_TRASH"
              />
            </template>
          </CButton>
        </template>

        {{ t('actionDelete') }}
      </CTooltip>

      <CTooltip
        v-if="userReplication"
        :delay="1000"
      >
        <template #trigger>
          <CButton
            secondary
            size="tiny"
            type="error"
            :loading="userReplicationToDelete?.isProcessing || false"
            @click="handleUserReplicationDelete"
          >
            <template #icon>
              <CIcon
                :is-inline="true"
                :name="IconName.BASE_PERSON_REMOVE"
              />
            </template>
          </CButton>
        </template>

        {{ t('actionDeleteUserReplication') }}
      </CTooltip>
    </div>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .replication-actions-cell {
    .actions-list {
      display: flex;
      gap: utils.unit(1);
    }
  }
</style>
