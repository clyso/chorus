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
  import { CAvatar, CResult, I18nLocale } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import ChorusUserCard from '@/components/chorus/common/ChorusUserCard/ChorusUserCard.vue';

  interface Props {
    users: string[];
    modelValue: string | null;
    emptyMessage?: string;
    emptyMessageTitle?: string;
  }

  defineProps<Props>();

  const { t } = useI18n({
    messages: {
      [I18nLocale.EN]: {
        noUsersTitle: 'No users',
        noUsers: 'No users found.',
      },
      [I18nLocale.DE]: {
        noUsersTitle: 'Keine Benutzer',
        noUsers: 'Keine Benutzer gefunden.',
      },
    },
  });

  const emit = defineEmits<{
    (e: 'update:modelValue', value: string | null): void;
  }>();

  function selectUser(user: string) {
    emit('update:modelValue', user);
  }
</script>

<template>
  <div
    v-if="users.length"
    class="user-list"
  >
    <ChorusUserCard
      v-for="user in users"
      :key="user"
      :user="user"
      :is-selectable="true"
      :is-selected="user === modelValue"
      @select="selectUser(user)"
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

  <CResult
    v-else
    status="error"
    type="error"
    :max-width="600"
    :has-positive="false"
    class="user-list__error"
  >
    <template #title>
      {{ emptyMessageTitle ?? t('noUsersTitle') }}
    </template>

    <p>
      {{ emptyMessage ?? t('noUsers') }}
    </p>
  </CResult>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .user-list {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
    gap: utils.unit(3);
  }
</style>
