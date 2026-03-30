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
  import i18nRoutingPolicies from '../i18nRoutingPolicies';
  import ChorusStorageTag from '../../common/ChorusStorageTag/ChorusStorageTag.vue';
  import type { RoutingPolicy } from '@/utils/types/chorus';
  import { IconName } from '@/utils/types/icon';

  const { t } = useI18n({
    messages: i18nRoutingPolicies,
  });

  defineProps<{
    routingPolicy: RoutingPolicy;
  }>();
</script>

<template>
  <div class="routing-policy-storage-cell">
    <ChorusStorageTag
      v-if="routingPolicy.toStorage"
      :storage-name="routingPolicy.toStorage"
      :tooltip="t('routingPolicyStorage')"
      type="primary"
      :icon-name="IconName.BASE_SERVER"
    />
  </div>
</template>
