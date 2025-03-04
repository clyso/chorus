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
  import { RouterView } from 'vue-router';
  import {
    CConfigProvider,
    CDialogProvider,
    CNotificationProvider,
  } from '@clyso/clyso-ui-kit';
  import { toRefs } from 'vue';
  import { useI18nStore } from '@/stores/i18nStore';
  import { useColorSchemeStore } from '@/stores/colorSchemeStore';

  const { locale } = toRefs(useI18nStore());
  const { colorScheme } = toRefs(useColorSchemeStore());
</script>

<template>
  <CConfigProvider
    class="app"
    :color-scheme="colorScheme"
    :locale="locale"
  >
    <RouterView />

    <CDialogProvider />
    <CNotificationProvider />
  </CConfigProvider>
</template>

<style lang="scss" scoped>
  .app {
    min-height: 100vh;
  }
</style>
