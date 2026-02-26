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
  import {
    CDescriptionItem,
    CDescriptionList,
    CDialog,
    CInput,
    DASHBOARD_NAV_META_INJECT_KEY,
    getDefaultNavMeta,
    I18nLocale,
  } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import { inject, readonly, ref } from 'vue';
  import { type ChorusProxyCredentials } from '@/utils/types/chorus';
  import { IconName } from '@/utils/types/icon';
  import CopyButton from '@/components/chorus/common/CopyButton/CopyButton.vue';

  const { t } = useI18n({
    messages: {
      [I18nLocale.EN]: {
        title: 'S3 Proxy Credentials',
        aliasLabel: 'Principal ID',
        accessKeyLabel: 'Access Key',
        secretKeyLabel: 'Secret Key',
      },
      [I18nLocale.DE]: {
        title: 'S3-Proxy-Anmeldeinformationen',
        aliasLabel: 'Principal ID',
        accessKeyLabel: 'Zugriffsschlüssel',
        secretKeyLabel: 'Geheimer Schlüssel',
      },
    },
  });

  defineProps<{
    isShown: boolean;
    proxy: ChorusProxyCredentials;
  }>();

  const emit = defineEmits<{
    (e: 'update:isShown', value: boolean): void;
  }>();

  const navMeta = inject(
    DASHBOARD_NAV_META_INJECT_KEY,
    readonly(ref(getDefaultNavMeta())),
  );
</script>

<template>
  <CDialog
    type="confirm"
    :has-negative="false"
    :icon-name="IconName.BASE_KEY"
    :is-shown="isShown"
    class="proxy-credentials-modal"
    @update:is-shown="(event: boolean) => emit('update:isShown', event)"
  >
    <template #title>
      <span>{{ t('title') }}</span>
    </template>

    <div class="credentials">
      <CDescriptionList
        class="credentials__item"
        :label-placement="navMeta.isMobile ? 'top' : 'left'"
        :columns="1"
      >
        <CDescriptionItem class="credentials__item-access-key">
          <template #label> {{ t('address') }}: </template>

          <div class="key-item">
            <span>{{ proxy.address }}</span>

            <CopyButton
              size="tiny"
              :text="proxy.address"
            />
          </div>
        </CDescriptionItem>
      </CDescriptionList>

      <CDescriptionList
        v-for="credentialsItem in proxy.credentials"
        :key="credentialsItem.alias"
        class="credentials__item"
        :label-placement="navMeta.isMobile ? 'top' : 'left'"
        :columns="1"
      >
        <CDescriptionItem class="credentials__item-id">
          <template #label> {{ t('aliasLabel') }}: </template>

          {{ credentialsItem.alias }}
        </CDescriptionItem>
        <CDescriptionItem class="credentials__item-access-key">
          <template #label> {{ t('accessKeyLabel') }}: </template>

          <div class="key-item">
            <span>{{ credentialsItem.accessKey }}</span>

            <CopyButton
              size="tiny"
              :text="credentialsItem.accessKey"
            />
          </div>
        </CDescriptionItem>
        <CDescriptionItem class="credentials__item-secret-key">
          <template #label> {{ t('secretKeyLabel') }}: </template>

          <div class="key-item">
            <CInput
              readonly
              size="small"
              show-password-on="click"
              :value="credentialsItem.secretKey"
              type="password"
            />

            <CopyButton
              size="tiny"
              :text="credentialsItem.secretKey"
            />
          </div>
        </CDescriptionItem>
      </CDescriptionList>
    </div>

    <template #positive-text>{{ 'OK' }}</template>
  </CDialog>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .credentials {
    &__item {
      padding: utils.unit(4) 0;
      border-bottom: 1px solid var(--border-color);

      &:first-child {
        padding-top: 0;
      }

      &:last-child {
        padding-bottom: 0;
        border: 0;
      }
    }

    &__item-secret-key {
      align-items: center;
    }
  }

  .key-item {
    display: flex;
    align-items: center;
    gap: utils.unit(1);
  }
</style>
