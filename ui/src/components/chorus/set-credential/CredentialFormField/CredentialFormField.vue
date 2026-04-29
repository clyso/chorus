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
  import { CInput, CFormField } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import { computed } from 'vue';
  import { storeToRefs } from 'pinia';
  import {
    useChorusSetCredentialStore,
    type CredentialFieldName,
  } from '@/stores/chorusSetCredentialStore';
  import i18nSetCredential from '@/components/chorus/set-credential/i18nSetCredential';

  type CInputProps = InstanceType<typeof CInput>['$props'];

  const labelTranslationKeys: Record<CredentialFieldName, string> = {
    user: 'fieldUserLabel',
    accessKey: 'fieldAccessKeyLabel',
    secretKey: 'fieldSecretKeyLabel',
    username: 'fieldUsernameLabel',
    password: 'fieldPasswordLabel',
    domainName: 'fieldDomainNameLabel',
    tenantName: 'fieldTenantNameLabel',
  };

  const props = withDefaults(
    defineProps<{
      fieldName: CredentialFieldName;
      type?: CInputProps['type'];
      disabled?: boolean;
    }>(),
    {
      type: 'text',
      disabled: false,
    },
  );

  const { t } = useI18n({ messages: i18nSetCredential });

  const storeRefs = storeToRefs(useChorusSetCredentialStore());

  const fieldValue = computed({
    get: () => storeRefs[props.fieldName].value,
    set: (value: string) => {
      storeRefs[props.fieldName].value = value;
    },
  });

  const validatorField = computed(
    () => storeRefs.validator.value[props.fieldName],
  );

  const labelKey = computed(() => labelTranslationKeys[props.fieldName]);

  const fieldId = computed(
    () => `credential-${props.fieldName.toLowerCase()}-input`,
  );
</script>

<template>
  <CFormField
    :field-id="fieldId"
    :has-error="validatorField.$error"
  >
    <template #label>
      {{ t(labelKey) }}
    </template>

    <template #default="{ hasError, fieldId: id }">
      <CInput
        :id="id"
        v-model:value="fieldValue"
        :has-error="hasError"
        :type="type"
        :disabled="disabled"
        @blur="validatorField.$touch()"
      />
    </template>

    <template #errors>
      <template v-if="validatorField.$error">
        {{ validatorField.$errors[0]?.$message }}
      </template>
    </template>
  </CFormField>
</template>
