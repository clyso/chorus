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
  import { CTile, CButton } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import { storeToRefs } from 'pinia';
  import { useChorusSetCredentialStore } from '@/stores/chorusSetCredentialStore';
  import SetCredentialConfirmDialog from '@/components/chorus/set-credential/SetCredentialConfirmDialog/SetCredentialConfirmDialog.vue';
  import i18nSetCredential from '@/components/chorus/set-credential/i18nSetCredential';
  import CredentialFormField from '@/components/chorus/set-credential/CredentialFormField/CredentialFormField.vue';

  const { t } = useI18n({
    messages: i18nSetCredential,
  });

  const {
    storage,
    user,
    isEditMode,
    isS3,
    isSwift,
    isSubmitting,
    isConfirmDialogOpen,
    validator,
  } = storeToRefs(useChorusSetCredentialStore());

  async function openConfirmDialog() {
    validator.value.$touch();

    const isValid = await validator.value.$validate();

    if (isValid) {
      isConfirmDialogOpen.value = true;
    }
  }
</script>

<template>
  <CTile
    class="credential-form-tile"
    :is-processing="isSubmitting"
  >
    <template #title>
      {{
        isEditMode
          ? t('setCredentialTitleEdit', { user })
          : t('setCredentialTitleAdd')
      }}
    </template>

    <template #header>
      {{ t('setCredentialHeader', { storage: storage?.name }) }}
    </template>

    <div class="credential-form-tile__content">
      <CredentialFormField
        field-name="user"
        :disabled="isEditMode"
      />

      <template v-if="isS3">
        <CredentialFormField field-name="accessKey" />
        <CredentialFormField
          field-name="secretKey"
          type="password"
        />
      </template>

      <template v-if="isSwift">
        <CredentialFormField field-name="username" />
        <CredentialFormField
          field-name="password"
          type="password"
        />
        <CredentialFormField field-name="domainName" />
        <CredentialFormField field-name="tenantName" />
      </template>

      <div class="credential-form-tile__actions">
        <CButton
          type="primary"
          size="large"
          :disabled="isSubmitting"
          @click="openConfirmDialog"
        >
          {{ t('actionSetCredential') }}
        </CButton>
      </div>
    </div>
  </CTile>
  <SetCredentialConfirmDialog />
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .credential-form-tile {
    &__content {
      display: flex;
      flex-direction: column;
    }

    &__actions {
      display: flex;
      justify-content: flex-start;
      padding-top: utils.unit(2);
    }
  }
</style>
