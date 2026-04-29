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
  import { storeToRefs } from 'pinia';
  import { CDialog } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import { h } from 'vue';
  import { useRouter } from 'vue-router';
  import { useChorusSetCredentialStore } from '@/stores/chorusSetCredentialStore';
  import { useChorusNotification } from '@/utils/composables/useChorusNotification';
  import { RouteName } from '@/utils/types/router';
  import i18nSetCredential from '@/components/chorus/set-credential/i18nSetCredential';

  const store = useChorusSetCredentialStore();
  const { isConfirmDialogOpen, storage } = storeToRefs(store);
  const { createNotification } = useChorusNotification();

  const { t } = useI18n({
    messages: i18nSetCredential,
  });

  const router = useRouter();

  async function submitCredentials() {
    if (!storage.value) {
      return;
    }

    try {
      await store.submitCredentials();

      createNotification({
        type: 'success',
        title: t('successTitle'),
        content: t('successContent'),
        duration: 4000,
      });

      router.push({
        name: RouteName.CHORUS_STORAGE_DETAILS,
        params: { storageName: storage.value.name },
      });

      isConfirmDialogOpen.value = false;
    } catch (error: unknown) {
      createNotification({
        type: 'error',
        title: t('errorTitle'),
        positiveText: t('errorAction'),
        positiveHandler: () => {
          submitCredentials();
        },
        content: () =>
          h('div', [
            t('errorContent'),
            h('br'),
            h('br'),
            h(
              'span',
              { style: 'white-space: pre-wrap' },
              error instanceof Error ? error.message : String(error),
            ),
          ]),
      });
    }
  }
</script>

<template>
  <CDialog
    class="set-credential-confirm-dialog"
    type="confirm"
    :width="600"
    :positive-handler="submitCredentials"
    v-model:is-shown="isConfirmDialogOpen"
  >
    <template #title>
      {{ t('confirmTitle') }}
    </template>

    <p>{{ t('confirmDescription') }}</p>

    <template #positive-text>
      {{ t('confirmPositive') }}
    </template>
    <template #negative-text>
      {{ t('confirmNegative') }}
    </template>
  </CDialog>
</template>
