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
  import { CButton, CIcon, CTooltip } from '@clyso/clyso-ui-kit';
  import type { RouteLocationRaw } from 'vue-router';

  const props = withDefaults(
    defineProps<{
      iconName: string;
      tooltipText: string;
      to?: RouteLocationRaw;
      isClickable?: boolean;
      isLoading?: boolean;
      type?: 'default' | 'success' | 'warning' | 'error' | 'info' | 'primary';
    }>(),
    {
      to: undefined,
      isClickable: false,
      isLoading: false,
      type: 'default',
    },
  );

  const emit = defineEmits<{
    (e: 'click'): void;
  }>();

  function handleClick() {
    if (!props.isClickable) {
      return;
    }

    emit('click');
  }
</script>

<template>
  <CTooltip :disable="!tooltipText">
    <template #trigger>
      <component
        :is="to ? 'RouterLink' : 'div'"
        :to="to"
        class="home-widget-action"
      >
        <CButton
          class="home-widget-action__button"
          secondary
          circle
          :tag="to ? 'span' : undefined"
          :tabindex="to ? '-1' : '0'"
          :loading="isLoading"
          size="small"
          :type="type"
          @click="handleClick"
        >
          <template #icon>
            <CIcon
              :is-inline="true"
              :name="iconName"
            />
          </template>
        </CButton>
      </component>
    </template>

    {{ tooltipText }}
  </CTooltip>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .home-widget-action {
    display: inline-flex;
    border-radius: 50%;
    border: 0;
    outline: 2px solid transparent;
    transform-style: outline utils.$transition-duration;

    &:focus-visible {
      outline-color: var(--primary-color) !important;
    }
  }
</style>
