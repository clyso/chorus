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
  import { computed, type Ref } from 'vue';
  import { storeToRefs } from 'pinia';
  import { useI18n } from 'vue-i18n';
  import { CSwitch } from '@clyso/clyso-ui-kit';
  import { useChorusAddDiffReportStore } from '@/stores/chorusAddDiffReportStore';
  import i18nAddDiffReport from '@/components/chorus/add-diff-report/i18nAddDiffReport';

  const { checkOnlyLastVersions, ignoreEtags, ignoreSizes } = storeToRefs(
    useChorusAddDiffReportStore(),
  );

  const { t } = useI18n({ messages: i18nAddDiffReport });

  interface SettingItem {
    ref: Ref<boolean, boolean>;
    labelKey: string;
    descriptionKey: string;
  }

  const settings = computed<SettingItem[]>(() => [
    {
      ref: checkOnlyLastVersions,
      labelKey: 'checkOnlyLastVersionsLabel',
      descriptionKey: 'checkOnlyLastVersionsDescription',
    },
    {
      ref: ignoreEtags,
      labelKey: 'ignoreEtagsLabel',
      descriptionKey: 'ignoreEtagsDescription',
    },
    {
      ref: ignoreSizes,
      labelKey: 'ignoreSizesLabel',
      descriptionKey: 'ignoreSizesDescription',
    },
  ]);
</script>

<template>
  <div class="additional-settings-step">
    <h5 class="additional-settings-step__title">
      {{ t('additionalSettingsTitle') }}
    </h5>

    <div class="additional-settings-step__items">
      <div
        v-for="setting in settings"
        :key="setting.labelKey"
        class="additional-settings-step__item"
      >
        <CSwitch
          size="small"
          label-placement="end"
          :value="setting.ref.value"
          @update:value="setting.ref.value = !setting.ref.value"
        >
          {{ t(setting.labelKey) }}
        </CSwitch>
        <p class="additional-settings-step__item-description">
          {{ t(setting.descriptionKey) }}
        </p>
      </div>
    </div>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .additional-settings-step {
    &__title {
      margin-bottom: utils.unit(4);
    }

    &__items {
      display: flex;
      flex-direction: column;
      gap: utils.unit(4);
    }

    &__item {
      cursor: pointer;
    }

    &__item-description {
      color: var(--text-color-3);
      font-size: 12px;
    }
  }
</style>
