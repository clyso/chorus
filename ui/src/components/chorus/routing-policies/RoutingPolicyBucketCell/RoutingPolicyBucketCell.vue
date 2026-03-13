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
  import { CTag, CTooltip } from '@clyso/clyso-ui-kit';
  import i18nRoutingPolicies from '../i18nRoutingPolicies';
  import type { RoutingPolicy } from '@/utils/types/chorus';

  const { t } = useI18n({
    messages: i18nRoutingPolicies,
  });

  const props = defineProps<{
    routingPolicy: RoutingPolicy;
  }>();

  const disableTooltip = props.routingPolicy.bucket !== '*';
</script>

<template>
  <div class="routing-policy-bucket-cell">
    <CTooltip
      :delay="500"
      :disabled="disableTooltip"
    >
      <template #trigger>
        <CTag
          :bordered="false"
          round
          size="small"
          class="routing-policy-bucket-cell__bucket"
          type="default"
        >
          {{ routingPolicy.bucket }}
        </CTag>
      </template>
      <span class="routing-policy-bucket-cell__bucket-tooltip">
        {{ t('allBuckets') }}
      </span>
    </CTooltip>
  </div>
</template>
