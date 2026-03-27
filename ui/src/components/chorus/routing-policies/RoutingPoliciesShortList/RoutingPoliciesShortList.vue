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
  import { CShortList } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import { computed } from 'vue';
  import i18nRoutingPolicies from '../i18nRoutingPolicies';
  import type { RoutingPolicy } from '@/utils/types/chorus';

  const { t } = useI18n({
    messages: i18nRoutingPolicies,
  });

  const props = withDefaults(
    defineProps<{
      routingPolicies: RoutingPolicy[] | [RoutingPolicy, string][];
      max?: number;
      size?: 'small' | 'medium';
    }>(),
    {
      max: 5,
      size: 'small',
    },
  );

  interface NormalizedRoutingPolicyItem {
    id: string;
    routingPolicy: RoutingPolicy;
    error: string | null;
  }

  const normalizedRoutingPoliciesList = computed(
    (): NormalizedRoutingPolicyItem[] => {
      return props.routingPolicies.map((routingPolicy) => {
        if (Array.isArray(routingPolicy)) {
          return {
            id: routingPolicy[0].id,
            routingPolicy: routingPolicy[0],
            error: routingPolicy[1],
          };
        }

        return {
          id: routingPolicy.id,
          routingPolicy: routingPolicy,
          error: null,
        };
      });
    },
  );
</script>

<template>
  <CShortList
    :size="size"
    :max="max"
    :list="normalizedRoutingPoliciesList"
    :item-key="(item) => item.id"
    class="routing-policies-short-list"
  >
    <template #default="{ item }: { item: NormalizedRoutingPolicyItem }">
      <strong>
        <span class="routing-policies-short-list__user"
          >{{ item.routingPolicy.user }}:
        </span>
      </strong>
      {{ item.routingPolicy.toStorage }}({{
        item.routingPolicy.bucket === '*'
          ? t('allBuckets')
          : item.routingPolicy.bucket
      }}) [{{
        item.routingPolicy.isBlocked ? t('statusBlocked') : t('statusAllowed')
      }}]{{ item.error ? ', ' : '' }}
      <span
        class="routing-policies-short-list__error"
        v-if="item.error"
      >
        <strong>
          <span class="routing-policies-short-list__details"
            >{{ t('deleteErrorDetailsLabel') }}:</span
          >
        </strong>
        {{ item.error }}
      </span>
    </template>
  </CShortList>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .routing-policies-short-list {
    &__user {
      font-weight: utils.$font-weight-medium;
    }

    &__details {
      font-weight: utils.$font-weight-medium;
    }
  }
</style>
