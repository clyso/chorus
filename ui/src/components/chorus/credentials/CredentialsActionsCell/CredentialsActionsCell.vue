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
  import { CButton, CTooltip, CIcon } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import { storeToRefs } from 'pinia';
  import i18nCredentials from '../i18nCredentials';
  import { RouteName } from '@/utils/types/router';
  import { IconName } from '@/utils/types/icon';
  import type { ChorusCredential } from '@/utils/types/chorus';
  import { useChorusStorageDetailsStore } from '@/stores/chorusStorageDetailsStore';

  const { t } = useI18n({
    messages: i18nCredentials,
  });

  const { storage } = storeToRefs(useChorusStorageDetailsStore());

  const props = defineProps<{
    credential: ChorusCredential;
  }>();
</script>

<template>
  <div class="credentials-actions-cell">
    <div class="actions-list">
      <div class="actions-list__item actions-list__item--edit">
        <CTooltip :delay="1000">
          <template #trigger>
            <RouterLink
              :to="{
                name: RouteName.CHORUS_SET_CREDENTIAL,
                params: {
                  storageName: storage?.name,
                  alias: props.credential.alias,
                },
              }"
            >
              <CButton
                secondary
                size="tiny"
                type="primary"
              >
                <template #icon>
                  <CIcon
                    :is-inline="true"
                    :name="IconName.BASE_CREATE"
                  />
                </template>
              </CButton>
            </RouterLink>
          </template>
          {{ t('actionEdit') }}
        </CTooltip>
      </div>
    </div>
  </div>
</template>
