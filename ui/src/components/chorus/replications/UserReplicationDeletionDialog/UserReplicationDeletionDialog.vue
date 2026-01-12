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
  import { CDialog, CCheckbox } from '@clyso/clyso-ui-kit';
  import { storeToRefs } from 'pinia';
  import { useI18n } from 'vue-i18n';
  import { useChorusReplicationsStore } from '@/stores/chorusReplicationsStore';
  import { IconName } from '@/utils/types/icon';
  import ReplicationsShortList from '@/components/chorus/replications/ReplicationsShortList/ReplicationsShortList.vue';
  import i18nReplications from '@/components/chorus/replications/i18nReplications';

  const { t } = useI18n({
    messages: i18nReplications,
  });

  const { userReplicationToDelete } = storeToRefs(useChorusReplicationsStore());

  const {
    setUserReplicationToDeleteConfirmation,
    setUserReplicationToDelete,
    deleteUserReplication,
  } = useChorusReplicationsStore();

  function handleLeave() {
    if (userReplicationToDelete.value?.hasError) {
      return;
    }

    setUserReplicationToDelete(null);
  }
</script>

<template>
  <CDialog
    class="user-replication-deletion-dialog"
    type="error"
    :icon-name="IconName.BASE_PERSON_REMOVE"
    :is-shown="userReplicationToDelete?.isConfirmationShown || false"
    :positive-handler="deleteUserReplication"
    @update:is-shown="setUserReplicationToDeleteConfirmation"
    @after-leave="handleLeave"
  >
    <template #title>
      {{ t('userDeletionConfirmTitle') }}
    </template>

    <div
      v-if="userReplicationToDelete"
      class="dialog-content"
    >
      <p>
        {{ t('userDeletionConfirmContent') }}
        <span class="dialog-content__user">{{
          userReplicationToDelete.replication.user
        }}</span
        >.
        {{ t('userDeletionConfirmContent2') }}
      </p>
      <p>
        {{ t('userDeletionConfirmQuestion') }}
      </p>

      <CCheckbox
        v-model:checked="userReplicationToDelete.isBucketsDeletion"
        class="dialog-content__buckets-checkbox"
      >
        {{
          t('userDeletionBucketsConfirmContent', {
            total: userReplicationToDelete.linkedBucketReplications.length,
          })
        }}
      </CCheckbox>

      <ReplicationsShortList
        size="medium"
        :replications="userReplicationToDelete.linkedBucketReplications"
      />
    </div>

    <template #positive-text>
      {{ t('userDeletionConfirmAction') }}
    </template>
    <template #negative-text>
      {{ t('userDeletionConfirmCancel') }}
    </template>
  </CDialog>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .dialog-content {
    display: grid;
    gap: utils.unit(2);

    &__user {
      font-weight: utils.$font-weight-semibold;
    }

    &__buckets-checkbox {
      margin-top: utils.unit(3);
    }
  }
</style>
