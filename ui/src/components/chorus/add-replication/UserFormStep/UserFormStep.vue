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
  import { computed } from 'vue';
  import { useI18n } from 'vue-i18n';
  import ChorusUserCardList from '../../common/ChorusUserCardList/ChorusUserCardList.vue';
  import { useChorusAddReplicationStore } from '@/stores/chorusAddReplicationStore';
  import i18nAddReplication from '@/components/chorus/add-replication/i18nAddReplication';

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

    <ChorusUserCardList
      v-model="selectedUser"
      :users="userOptions"
    />
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .user-form-step {
    &__title {
      margin-bottom: utils.unit(2);
    }
  }
</style>
