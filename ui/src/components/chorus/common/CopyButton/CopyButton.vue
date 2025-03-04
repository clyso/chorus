<!--
  - Copyright © 2025 Clyso GmbH
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
  import { CCopyButton, I18nLocale } from '@clyso/clyso-ui-kit';

  withDefaults(
    defineProps<{
      text: string;
      size?: 'tiny' | 'small' | 'medium' | 'large';
    }>(),
    {
      size: 'tiny',
    },
  );

  const { t } = useI18n({
    messages: {
      [I18nLocale.EN]: {
        copied: 'Copied!',
        failed: 'Copy failed!',
      },
      [I18nLocale.DE]: {
        copied: 'Kopiert!',
        failed: 'Kopieren fehlgeschlagen!',
      },
    },
  });
</script>

<template>
  <CCopyButton
    class="copy-button"
    :size="size"
    :text="text"
    :success-text="t('copied')"
    :error-text="t('failed')"
  />
</template>
