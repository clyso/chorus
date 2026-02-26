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
  import { computed, watch } from 'vue';
  import { CResult, CSelect, I18nLocale } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';

  const { t } = useI18n({
    messages: {
      [I18nLocale.EN]: {
        noResultTitle: 'No Data Found',
        noResultText: 'No storage matches your search criteria.',
      },
      [I18nLocale.DE]: {
        noResultTitle: 'Keine Daten gefunden',
        noResultText: 'Keine Speicher entspricht Ihren Suchkriterien.',
      },
    },
  });

  const props = defineProps<{
    storages: string[];
    placeholder: string;
    noResultTitle?: string;
    noResultText?: string;
  }>();

  const noResultTitleValue = computed(() =>
    props.noResultTitle ? props.noResultTitle : t('noResultTitle'),
  );
  const noResultTextValue = computed(() =>
    props.noResultText ? props.noResultText : t('noResultText'),
  );

  const filterValue = defineModel<string[]>('filterValue', { required: true });

  const storageOptions = computed<
    {
      label: string;
      value: string;
    }[]
  >(() =>
    [...new Set(props.storages)].sort().map((storage) => ({
      label: storage,
      value: storage,
    })),
  );

  watch(storageOptions, () => {
    const validLabels = storageOptions.value.map((s) => s.label);

    filterValue.value = filterValue.value.filter((v) =>
      validLabels.includes(v),
    );
  });
</script>

<template>
  <CSelect
    v-model:value="filterValue"
    class="chorus-storage-filter"
    multiple
    filterable
    clearable
    :placeholder="placeholder"
    :options="storageOptions"
    :max-tag-count="1"
  >
    <template #empty>
      <CResult
        type="empty"
        size="tiny"
        v-if="!storageOptions.length"
      >
        <template #title>
          {{ noResultTitleValue }}
        </template>

        <p>{{ noResultTextValue }}</p>
      </CResult>
    </template>
  </CSelect>
</template>
