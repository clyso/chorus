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
  import { computed } from 'vue';
  import { CSelect } from '@clyso/clyso-ui-kit';
  import { storeToRefs } from 'pinia';
  import i18nRoutingPolicies from '../i18nRoutingPolicies';
  import { RoutingPolicyStatusFilter } from '@/utils/types/chorus';
  import { useChorusRoutingPoliciesStore } from '@/stores/chorusRoutingPoliciesStore';

  const { t } = useI18n({
    messages: i18nRoutingPolicies,
  });

  const { filterStatus, page } = storeToRefs(useChorusRoutingPoliciesStore());

  const statusOptions = computed<
    {
      value: RoutingPolicyStatusFilter;
      label: string;
    }[]
  >(() => [
    {
      value: RoutingPolicyStatusFilter.ALLOWED,
      label: t('filterStatusAllowed'),
    },
    {
      value: RoutingPolicyStatusFilter.BLOCKED,
      label: t('filterStatusBlocked'),
    },
  ]);
</script>

<template>
  <CSelect
    v-model:value="filterStatus"
    class="routing-policies-filter-by-status"
    clearable
    :placeholder="t('filterByStatusPlaceholder')"
    :options="statusOptions"
    :max-tag-count="1"
    @update:value="page = 1"
  />
</template>
