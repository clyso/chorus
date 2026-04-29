/*
 * Copyright © 2026 Clyso GmbH
 *
 *  Licensed under the GNU Affero General Public License, Version 3.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  https://www.gnu.org/licenses/agpl-3.0.html
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import useVuelidate from '@vuelidate/core';
import { defineStore } from 'pinia';
import { computed, reactive, toRefs } from 'vue';
import { helpers } from '@vuelidate/validators';
import { useI18n } from 'vue-i18n';
import { useRouter } from 'vue-router';
import {
  StorageProvider,
  type ChorusStorage,
  type UserCredentialSetRequest,
} from '@/utils/types/chorus';
import { ChorusService } from '@/services/ChorusService';
import { RouteName } from '@/utils/types/router';
import { ErrorHelper } from '@/utils/helpers/ErrorHelper';
import i18nSetCredential from '@/components/chorus/set-credential/i18nSetCredential';

interface ChorusSetCredentialState {
  isLoading: boolean;
  isSubmitting: boolean;
  isEditMode: boolean;
  hasError: boolean;
  isConfirmDialogOpen: boolean;
  storage: ChorusStorage | null;
  user: string;

  accessKey: string;
  secretKey: string;

  username: string;
  password: string;
  domainName: string;
  tenantName: string;
}

export type CredentialFieldName = keyof Pick<
  ChorusSetCredentialState,
  | 'user'
  | 'accessKey'
  | 'secretKey'
  | 'username'
  | 'password'
  | 'domainName'
  | 'tenantName'
>;

function getInitialState(): ChorusSetCredentialState {
  return {
    isLoading: false,
    isSubmitting: false,
    isEditMode: false,
    hasError: false,
    isConfirmDialogOpen: false,
    storage: null,
    user: '',

    accessKey: '',
    secretKey: '',

    username: '',
    password: '',
    domainName: '',
    tenantName: '',
  };
}

export const useChorusSetCredentialStore = defineStore(
  'chorusSetCredential',
  () => {
    const state = reactive<ChorusSetCredentialState>(getInitialState());
    const router = useRouter();

    const { t } = useI18n({
      messages: i18nSetCredential,
    });

    const isS3 = computed(() => state.storage?.provider === StorageProvider.S3);

    const isSwift = computed(
      () => state.storage?.provider === StorageProvider.SWIFT,
    );

    const validationRules = computed(() => ({
      user: {
        required: helpers.withMessage(
          t('validationUserRequired'),
          (value: string) => !!value.trim(),
        ),
      },
      accessKey: {
        required: helpers.withMessage(
          t('validationAccessKeyRequired'),
          (value: string) => !isS3.value || !!value.trim(),
        ),
      },
      secretKey: {
        required: helpers.withMessage(
          t('validationSecretKeyRequired'),
          (value: string) => !isS3.value || !!value.trim(),
        ),
      },
      username: {
        required: helpers.withMessage(
          t('validationUsernameRequired'),
          (value: string) => !isSwift.value || !!value.trim(),
        ),
      },
      password: {
        required: helpers.withMessage(
          t('validationPasswordRequired'),
          (value: string) => !isSwift.value || !!value.trim(),
        ),
      },
      domainName: {
        required: helpers.withMessage(
          t('validationDomainNameRequired'),
          (value: string) => !isSwift.value || !!value.trim(),
        ),
      },
      tenantName: {
        required: helpers.withMessage(
          t('validationTenantNameRequired'),
          (value: string) => !isSwift.value || !!value.trim(),
        ),
      },
    }));

    const validator = useVuelidate(validationRules, state);

    async function initSetCredentialPage(storageName: string, alias?: string) {
      state.isLoading = true;
      state.hasError = false;

      try {
        const { storages } = await ChorusService.getStorages();

        state.storage =
          storages.find(({ name }) => storageName === name) ?? null;

        if (!state.storage) {
          router.push({ name: RouteName.CHORUS_STORAGES });

          return;
        }

        if (alias) {
          state.user = alias;
          state.isEditMode = true;
        }
      } catch {
        state.hasError = true;
      } finally {
        state.isLoading = false;
      }
    }

    async function submitCredentials() {
      state.isSubmitting = true;

      try {
        if (!state.storage) {
          throw new Error(t('submitErrorUnknownStorage'));
        }

        const userCredential: UserCredentialSetRequest = {
          storage: state.storage.name,
          user: state.user,
        };

        if (isS3.value) {
          userCredential.s3Cred = {
            accessKey: state.accessKey,
            secretKey: state.secretKey,
          };
        }

        if (isSwift.value) {
          userCredential.swiftCred = {
            username: state.username,
            password: state.password,
            domainName: state.domainName,
            tenantName: state.tenantName,
          };
        }

        await ChorusService.setUserCredentials(userCredential);
      } catch (error: unknown) {
        throw new Error(
          ErrorHelper.getReason(error) || t('submitErrorUnknown'),
        );
      } finally {
        state.isSubmitting = false;
      }
    }

    function $reset() {
      Object.assign(state, getInitialState());
      validator.value.$reset();
    }

    return {
      ...toRefs(state),
      isS3,
      isSwift,
      validator,
      initSetCredentialPage,
      submitCredentials,
      $reset,
    };
  },
);
