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
  import { onBeforeMount, onUnmounted } from 'vue';
  import { useI18n } from 'vue-i18n';
  import { RouteName } from '@/utils/types/router';
  import { useChorusAddRoutingPolicyStore } from '@/stores/chorusAddRoutingPolicyStore';
  import AddRoutingPolicyTile from '@/components/chorus/add-routing-policies/AddRoutingPolicyTile/AddRoutingPolicyTile.vue';
  import i18nAddRoutingPolicy from '@/components/chorus/add-routing-policies/i18nAddRoutingPolicy';

  const { t } = useI18n({ messages: i18nAddRoutingPolicy });
  const { initAddRoutingPolicyPage, $reset } = useChorusAddRoutingPolicyStore();

  onBeforeMount(() => {
    initAddRoutingPolicyPage();
  });

  onUnmounted(() => {
    $reset();
  });
</script>

<template>
  <CDashboardPage class="chorus-add-routing-policy-page">
    <template #breadcrumbs>
      <CBreadcrumb>
        <CBreadcrumbItem :to="{ name: RouteName.CHORUS_ROUTING_POLICIES }">
          {{ t('breadcrumbRoutingPolicies') }}
        </CBreadcrumbItem>
        <CBreadcrumbItem :is-active="true">
          {{ t('breadcrumbAddRoutingPolicy') }}
        </CBreadcrumbItem>
      </CBreadcrumb>
    </template>

    <AddRoutingPolicyTile />
  </CDashboardPage>
</template>
