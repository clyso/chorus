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
    CBreadcrumb,
    CBreadcrumbItem,
    CDashboardPage,
  } from '@clyso/clyso-ui-kit';
  import { computed, onBeforeMount, onUnmounted } from 'vue';
  import { useI18n } from 'vue-i18n';
  import { RouteName } from '@/utils/types/router';
  import i18nSetCredential from '@/components/chorus/set-credential/i18nSetCredential';
  import { useChorusSetCredentialStore } from '@/stores/chorusSetCredentialStore';

  const props = defineProps<{
    storageName: string;
    alias?: string;
  }>();

  const { t } = useI18n({ messages: i18nSetCredential });
  const { initSetCredentialPage, $reset } = useChorusSetCredentialStore();

  const isEditMode = computed(() => !!props.alias);

  onBeforeMount(() => {
    initSetCredentialPage(props.storageName, props.alias);
  });

  onUnmounted(() => {
    $reset();
  });
</script>

<template>
  <CDashboardPage class="chorus-set-credential-page">
    <template #breadcrumbs>
      <CBreadcrumb>
        <CBreadcrumbItem :to="{ name: RouteName.CHORUS_STORAGES }">
          {{ t('breadcrumbStorages') }}
        </CBreadcrumbItem>
        <CBreadcrumbItem
          :to="{
            name: RouteName.CHORUS_STORAGE_DETAILS,
            params: { storageName: props.storageName },
          }"
        >
          {{ props.storageName }}
        </CBreadcrumbItem>
        <CBreadcrumbItem :is-active="true">
          {{
            isEditMode
              ? t('breadcrumbEditCredential')
              : t('breadcrumbAddCredential')
          }}
        </CBreadcrumbItem>
      </CBreadcrumb>
    </template>

    <!-- TODO: page content here-->
  </CDashboardPage>
</template>
