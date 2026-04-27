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
  import { storeToRefs } from 'pinia';
  import { useI18n } from 'vue-i18n';
  import i18nCredentials from '../i18nCredentials';
  import { useChorusStorageDetailsStore } from '@/stores/chorusStorageDetailsStore';

  const { t } = useI18n({
    messages: i18nCredentials,
  });

  const { storage } = storeToRefs(useChorusStorageDetailsStore());
</script>

<template>
  <div class="credentials-actions-cell">
    <div class="actions-list">
      <div class="actions-list__item actions-list__item--edit">
        <CTooltip :delay="1000">
          <template #trigger>
            <RouterLink
              :to="{
                // name & params: TODO: add route name, once defined
              }"
            >
              <CButton
                secondary
                size="tiny"
                type="primary"
                :loading="!storage"
              >
                <template #icon>
                  <CIcon
                    :is-inline="true"
                    name="base-create"
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
