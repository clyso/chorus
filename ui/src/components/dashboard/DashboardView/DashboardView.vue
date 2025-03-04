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
  import { storeToRefs } from 'pinia';
  import {
    CColorSchemeToggle,
    CDashboardHeader,
    CDashboardLayout,
    CLanguageSelect,
  } from '@clyso/clyso-ui-kit';
  import { useI18nStore } from '@/stores/i18nStore';
  import { useColorSchemeStore } from '@/stores/colorSchemeStore';
  import DashboardNav from '@/components/dashboard/DashboardNav/DashboardNav.vue';
  import DashboardLogo from '@/components/dashboard/DashboardLogo/DashboardLogo.vue';
  import { HAS_LANG_SELECT } from '@/utils/constants/env';
  import DashboardFooter from '@/components/dashboard/DashboardFooter/DashboardFooter.vue';

  const { locale } = storeToRefs(useI18nStore());
  const { setLocale } = useI18nStore();

  const { colorScheme } = storeToRefs(useColorSchemeStore());
  const { setColorScheme } = useColorSchemeStore();
</script>

<template>
  <CDashboardLayout
    class="dashboard-view"
    :has-side-menu="false"
  >
    <template #header>
      <CDashboardHeader
        :options="[]"
        :has-side-menu="false"
        :has-user-menu="false"
      >
        <template #start>
          <DashboardLogo />
        </template>

        <template #end>
          <CLanguageSelect
            v-if="HAS_LANG_SELECT"
            :value="locale"
            @update:value="setLocale"
          />

          <CColorSchemeToggle
            :value="colorScheme"
            @update:value="setColorScheme"
          />
        </template>
      </CDashboardHeader>
    </template>

    <main class="dashboard-view__main">
      <DashboardNav class="dashboard-view__nav" />
      <div class="dashboard-view__render-view">
        <RouterView />
      </div>
    </main>

    <template #footer>
      <DashboardFooter />
    </template>
  </CDashboardLayout>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .dashboard-view {
    &__main {
      height: 100%;
      max-width: 100vw;
      padding: utils.unit(5) utils.unit(8) utils.unit(12);
      display: grid;
      grid-template-rows: auto 1fr;

      @include utils.tablet-only {
        padding: utils.unit(5) utils.unit(6) utils.unit(12);
      }

      @include utils.mobile {
        padding: utils.unit(3) utils.unit(3) utils.unit(12);
      }
    }

    &__nav {
      margin-bottom: utils.unit(8);
      min-width: 0;
    }

    &__render-view {
      min-width: 0;
    }

    ::v-deep(.c-dashboard-header) {
      z-index: 2;
    }

    ::v-deep(.c-dashboard-layout__footer) {
      padding: 0;
    }
  }
</style>
