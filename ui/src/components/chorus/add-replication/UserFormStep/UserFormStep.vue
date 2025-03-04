<!--
  - Copyright © 2025 Clyso GmbH
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
  import { CAvatar } from '@clyso/clyso-ui-kit';
  import { computed } from 'vue';
  import { useI18n } from 'vue-i18n';
  import { useChorusAddReplicationStore } from '@/stores/chorusAddReplicationStore';
  import i18nAddReplication from '@/components/chorus/add-replication/i18nAddReplication';
  import ChorusUserCard from '@/components/chorus/common/ChorusUserCard/ChorusUserCard.vue';

  const { selectedFromStorage, selectedUser } = storeToRefs(
    useChorusAddReplicationStore(),
  );

  const { t } = useI18n({
    messages: i18nAddReplication,
  });

  const userOptions = computed(
    () =>
      selectedFromStorage.value?.credentials.map(({ alias }) => alias) ?? [],
  );
</script>

<template>
  <div class="user-form-step">
    <p class="user-form-step__title">
      {{ t('userStepTitle') }}
    </p>

    <div class="user-list">
      <ChorusUserCard
        v-for="user in userOptions"
        :key="user"
        :user="user"
        :is-selectable="true"
        :is-selected="user === selectedUser"
        @select="selectedUser = user"
      >
        <div class="user-list__option-inner">
          <CAvatar
            round
            :name="user"
            class="user-list__option-avatar"
          />

          <span class="user-list__option-name">{{ user }}</span>
        </div>
      </ChorusUserCard>
    </div>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .user-form-step {
    &__title {
      margin-bottom: utils.unit(2);
    }
  }

  .user-list {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
    gap: utils.unit(3);
  }
</style>
