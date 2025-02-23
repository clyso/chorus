<script setup lang="ts">
  import { useI18n } from 'vue-i18n';
  import { computed, reactive } from 'vue';
  import { required } from '@vuelidate/validators';
  import useVuelidate from '@vuelidate/core';
  import {
    CButton,
    CFormField,
    CForm,
    CAlert,
    CInput,
  } from '@clyso/clyso-ui-kit';
  import i18nLoginPage from '@/components/auth/LoginPage/i18nLoginPage';
  import { useAuthStore } from '@/stores/authStore';

  const { t } = useI18n({
    messages: i18nLoginPage,
  });

  const authStore = useAuthStore();

  const credentials = reactive({
    username: '',
    password: '',
  });

  const formStatus = reactive({
    isSubmitting: false,
    hasSubmitError: false,
  });

  const validationRules = computed(() => ({
    credentials: {
      username: {
        required,
      },
      password: {
        required,
      },
    },
  }));

  const v$ = useVuelidate(validationRules, {
    credentials,
  });

  async function submit() {
    v$.value.$touch();

    if (v$.value.$error) {
      return;
    }

    formStatus.hasSubmitError = false;
    formStatus.isSubmitting = true;

    try {
      await authStore.login({
        username: credentials.username,
        password: credentials.password,
      });
    } catch {
      formStatus.hasSubmitError = true;
    } finally {
      formStatus.isSubmitting = false;
    }
  }
</script>

<template>
  <CForm class="login-form">
    <CFormField
      field-id="login-username"
      :has-error="v$.credentials.username.$error"
    >
      <template #label>
        {{ t('username') }}
      </template>

      <template #default="{ hasError, fieldId }">
        <CInput
          :id="fieldId"
          v-model:value="credentials.username"
          size="large"
          :has-error="hasError"
          :placeholder="t('username')"
          @input="formStatus.hasSubmitError = false"
        />
      </template>

      <template #errors>
        {{ t('required', { field: t('username') }) }}
      </template>
    </CFormField>

    <CFormField
      field-id="login-password"
      :has-error="v$.credentials.password.$error"
    >
      <template #label>
        {{ t('password') }}
      </template>

      <template #default="{ hasError, fieldId }">
        <CInput
          :id="fieldId"
          v-model:value="credentials.password"
          :has-error="hasError"
          size="large"
          :placeholder="t('password')"
          type="password"
          show-password-on="click"
          @input="formStatus.hasSubmitError = false"
        />
      </template>

      <template #errors>
        {{ t('required', { field: t('password') }) }}
      </template>
    </CFormField>

    <template
      v-if="formStatus.hasSubmitError"
      #error
    >
      <CAlert
        type="error"
        closable
        @close="formStatus.hasSubmitError = false"
      >
        <template #header>
          {{ t('wrongCredentialsTitle') }}
        </template>
        {{ t('wrongCredentialsSubtitle') }}
      </CAlert>
    </template>

    <template #actions>
      <div class="form-actions">
        <CButton
          type="primary"
          size="large"
          block
          class="form-actions__login"
          :loading="formStatus.isSubmitting"
          attr-type="submit"
          @click="submit"
        >
          Login
        </CButton>

        <CButton
          class="form-actions__forgot"
          quaternary
        >
          {{ t('forgot') }}
        </CButton>
      </div>
    </template>
  </CForm>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .login-form {
    width: 100%;
    max-width: 400px;
  }

  .form-actions {
    display: flex;
    flex-direction: column;
    align-items: center;

    &__login {
      margin-bottom: utils.unit(3);
    }
  }
</style>
