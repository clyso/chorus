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
  import { useI18n } from 'vue-i18n';
  import { storeToRefs } from 'pinia';
  import { computed } from 'vue';
  import i18nRoutingPolicies from '../i18nRoutingPolicies';
  import { useChorusRoutingPoliciesStore } from '@/stores/chorusRoutingPoliciesStore';
  import ChorusUserFilter from '@/components/chorus/common/ChorusUserFilter/ChorusUserFilter.vue';

  const { t } = useI18n({
    messages: i18nRoutingPolicies,
  });

  const { routingPolicies, filterUsers, page } = storeToRefs(
    useChorusRoutingPoliciesStore(),
  );

  const users = computed(() =>
    routingPolicies.value.map((routingPolicy) => routingPolicy.user),
  );
</script>

<template>
  <ChorusUserFilter
    v-model:filterValue="filterUsers"
    :users="users"
    :placeholder="t('filterByUserPlaceholder')"
    @update:filter-value="page = 1"
  />
</template>
