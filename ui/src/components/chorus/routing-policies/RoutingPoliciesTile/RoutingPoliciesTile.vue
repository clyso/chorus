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
  import { CTile } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import i18nRoutingPolicies from '../i18nRoutingPolicies';
  import RoutingPoliciesList from '../RoutingPoliciesList/RoutingPoliciesList.vue';
  import RoutingPoliciesFilters from '../RoutingPoliciesFilters/RoutingPoliciesFilters.vue';
  import RoutingPoliciesListActions from '../RoutingPoliciesListActions/RoutingPoliciesListActions.vue';

  const { t } = useI18n({
    messages: i18nRoutingPolicies,
  });
</script>

<template>
  <CTile class="routing-policies-tile">
    <template #title>
      {{ t('routingPoliciesTitle') }}
    </template>

    <RoutingPoliciesFilters class="routing-policies-tile__filters" />

    <RoutingPoliciesListActions class="routing-policies-tile__actions" />

    <RoutingPoliciesList class="routing-policies-tile__routing-policies" />
  </CTile>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .routing-policies-tile {
    &__filters {
      margin-bottom: utils.unit(6);
    }

    &__tags {
      margin-bottom: utils.unit(4);
    }

    &__actions {
      margin-bottom: utils.unit(4);
    }
  }
</style>
