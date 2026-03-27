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
  import { CIcon, CTag, CTooltip } from '@clyso/clyso-ui-kit';
  import i18nRoutingPolicies from '../i18nRoutingPolicies';
  import { IconName } from '@/utils/types/icon';
  import type { RoutingPolicy } from '@/utils/types/chorus';

  const { t } = useI18n({
    messages: i18nRoutingPolicies,
  });

  defineProps<{
    routingPolicy: RoutingPolicy;
  }>();
</script>

<template>
  <div class="routing-policy-status-cell">
    <CTooltip :delay="500">
      <template #trigger>
        <CTag
          :bordered="false"
          round
          size="small"
          class="routing-policy-status-cell__status"
          :type="routingPolicy.isBlocked ? 'error' : 'success'"
        >
          <template #icon>
            <CIcon
              :is-inline="true"
              :name="
                routingPolicy.isBlocked
                  ? IconName.BASE_CLOSE_CIRCLE
                  : IconName.BASE_CHECKMARK
              "
            />
          </template>
          {{
            routingPolicy.isBlocked ? t('statusBlocked') : t('statusAllowed')
          }}
        </CTag>
      </template>
      <span class="routing-policy-status-cell__status-tooltip">
        <strong>{{
          routingPolicy.isBlocked ? t('statusBlocked') : t('statusAllowed')
        }}</strong
        >:
        {{
          routingPolicy.isBlocked
            ? t('routingPolicyStatusBlocked')
            : t('routingPolicyStatusAllowed')
        }}
      </span>
    </CTooltip>
  </div>
</template>
