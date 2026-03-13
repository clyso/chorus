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
  import { computed, watch } from 'vue';
  import { CResult, CSelect } from '@clyso/clyso-ui-kit';
  import i18nRoutingPolicies from '../i18nRoutingPolicies';
  import { useChorusRoutingPoliciesStore } from '@/stores/chorusRoutingPoliciesStore';

  const { t } = useI18n({
    messages: i18nRoutingPolicies,
  });

  const { routingPolicies, filterUsers, page } = storeToRefs(
    useChorusRoutingPoliciesStore(),
  );

  const usersOptions = computed<
    {
      label: string;
      value: string;
    }[]
  >(() => {
    const sortedUniqueUsers = [
      ...new Set(
        routingPolicies.value.map((routingPolicy) => routingPolicy.user),
      ),
    ].sort();

    return sortedUniqueUsers.map((user) => ({ label: user, value: user }));
  });

  function sanitizeFilterUsers() {
    filterUsers.value = filterUsers.value.filter((filterUser) =>
      usersOptions.value.map((user) => user.label).includes(filterUser),
    );
  }

  watch(usersOptions, sanitizeFilterUsers);
</script>

<template>
  <CSelect
    v-model:value="filterUsers"
    class="routing-policies-filter-by-user"
    multiple
    filterable
    clearable
    :placeholder="t('filterByUserPlaceholder')"
    :options="usersOptions"
    :max-tag-count="1"
    @update:value="page = 1"
  >
    <template #empty>
      <CResult
        type="empty"
        size="tiny"
      >
        <template v-if="usersOptions.length">
          {{ t('filterByUserNoResult') }}
        </template>
      </CResult>
    </template>
  </CSelect>
</template>
