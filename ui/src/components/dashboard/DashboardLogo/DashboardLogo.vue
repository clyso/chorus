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
  import { CIcon } from '@clyso/clyso-ui-kit';
  import { RouteName } from '@/utils/types/router';
  import { IconName } from '@/utils/types/icon';
</script>

<template>
  <div class="dashboard-logo">
    <RouterLink
      class="dashboard-logo__chorus"
      :to="{ name: RouteName.CHORUS_HOME }"
    >
      <CIcon
        :is-inline="true"
        class="dashboard-logo__chorus-icon"
        :name="IconName.BASE_CHORUS_LOGO"
      />
      <span class="dashboard-logo__chorus-text">Chorus</span>
    </RouterLink>

    <a
      href="https://clyso.com"
      rel="noopener"
      target="_blank"
      class="dashboard-logo__clyso"
    >
      <CIcon
        class="dashboard-logo__clyso-icon"
        :is-inline="true"
        :name="IconName.BASE_LOGO_CLYSO"
      />
    </a>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;
  @use 'sass:math';

  $chorus-icon-size: 36px;
  $chorus-logo-gap: utils.unit(6);

  .dashboard-logo {
    display: flex;
    align-items: center;
    gap: $chorus-logo-gap;

    &__chorus {
      display: flex;
      align-items: center;
      gap: utils.unit(3);
    }

    &__chorus-icon {
      width: 40px;
      height: 40px;
      color: var(--primary-color);
    }

    &__clyso {
      position: relative;

      &::before {
        @include utils.absolute-y-center;
        left: - math.div($chorus-logo-gap, 2);
        content: '';

        height: 24px;
        width: 1px;
        background: var(--text-color-base);
        opacity: 0.3;
        pointer-events: none;
      }
    }

    &__chorus-text {
      @include utils.apply-styles(utils.$text-h1);
      line-height: 1;
      gap: utils.unit(2);
      margin-bottom: -4px;
    }

    &__clyso-icon {
      height: 16px;
      width: auto;
      fill: var(--text-color-3);
      margin-bottom: -2px;
    }
  }
</style>
