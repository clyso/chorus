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
  withDefaults(
    defineProps<{
      legend: { id: string; color: string; label: string }[];
      flow: 'horizontal' | 'vertical';
    }>(),
    {
      flow: 'horizontal',
    },
  );
</script>

<template>
  <div
    :class="{ 'home-chart-legend--vertical': flow === 'vertical' }"
    class="home-chart-legend"
  >
    <ul class="home-chart-legend__list">
      <li
        v-for="item in legend"
        :key="item.id"
        class="home-chart-legend__item"
      >
        <span
          :style="{ 'background-color': item.color }"
          class="home-chart-legend__item-color"
        ></span>
        <span class="home-chart-legend__item-label">{{ item.label }}</span>
      </li>
    </ul>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .home-chart-legend {
    &--vertical {
      .home-chart-legend__list {
        flex-direction: column;
        align-items: flex-start;
      }
    }

    &__list {
      display: flex;
      align-items: center;
      flex-wrap: wrap;
      gap: 0 utils.unit(3);
    }

    &__item {
      display: flex;
      align-items: center;
      gap: utils.unit(2);
    }

    &__item-color {
      width: 12px;
      height: 12px;
      border-radius: 50%;
    }

    &__item-label {
      @include utils.apply-styles(utils.$text-small);
    }
  }
</style>
