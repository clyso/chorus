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
  import { useI18n } from 'vue-i18n';
  import { computed } from 'vue';
  import i18nCredentials from '../i18nCredentials';
  import ChorusUserFilter from '../../common/ChorusUserFilter/ChorusUserFilter.vue';
  import { useChorusStorageDetailsStore } from '@/stores/chorusStorageDetailsStore';

  const { t } = useI18n({
    messages: i18nCredentials,
  });

  const { credentialsList, credentialsFilterAliases, credentialsPage } =
    storeToRefs(useChorusStorageDetailsStore());

  const credentials = computed(() =>
    credentialsList.value.map((credential) => credential.alias),
  );
</script>

<template>
  <ChorusUserFilter
    v-model:filterValue="credentialsFilterAliases"
    :users="credentials"
    :placeholder="t('filterByUserAliasPlaceholder')"
    @update:filter-value="credentialsPage = 1"
  />
</template>
