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
  import { CButton, CIcon, CTile } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import { storeToRefs } from 'pinia';
  import i18nCredentials from '../i18nCredentials';
  import CredentialsList from '../CredentialsList/CredentialsList.vue';
  import CredentialsFilters from '../CredentialsFilters/CredentialsFilters.vue';
  import { RouteName } from '@/utils/types/router';
  import { IconName } from '@/utils/types/icon';
  import { useChorusStorageDetailsStore } from '@/stores/chorusStorageDetailsStore';

  const { t } = useI18n({
    messages: i18nCredentials,
  });

  const { storage } = storeToRefs(useChorusStorageDetailsStore());
</script>

<template>
  <CTile class="credentials-tile">
    <template #title>
      {{ t('credentialsTitle') }}
    </template>

    <CredentialsFilters class="credentials-tile__filters" />

    <div class="credentials-tile__actions">
      <div class="credentials-tile__action credentials-tile__action--creation">
        <RouterLink
          :to="{
            name: RouteName.CHORUS_SET_CREDENTIAL,
            params: { storageName: storage?.name },
          }"
        >
          <CButton
            type="primary"
            size="medium"
            ghost
            tag="div"
          >
            <template #icon>
              <CIcon
                :is-inline="true"
                :name="IconName.BASE_ADD"
              />
            </template>

            {{ t('addCredentialAction') }}
          </CButton>
        </RouterLink>
      </div>
    </div>

    <CredentialsList class="credentials-tile__credentials" />
  </CTile>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .credentials-tile {
    &__filters {
      margin-bottom: utils.unit(6);
    }

    &__actions {
      margin-bottom: utils.unit(4);
      display: flex;
      flex-direction: row-reverse;
    }
  }
</style>
