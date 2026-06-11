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
        noResultText: 'No bucket matches your search criteria.',
      },
      [I18nLocale.DE]: {
        noResultTitle: 'Keine Daten gefunden',
        noResultText: 'Kein Bucket entspricht Ihren Suchkriterien.',
      },
    },
  });

  const props = defineProps<{
    buckets: string[];
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

  const bucketOptions = computed<
    {
      label: string;
      value: string;
    }[]
  >(() =>
    [...new Set(props.buckets)].sort().map((bucket) => ({
      label: bucket,
      value: bucket,
    })),
  );

  watch(bucketOptions, () => {
    const validLabels = bucketOptions.value.map((bucket) => bucket.label);

    filterValue.value = filterValue.value.filter((value) =>
      validLabels.includes(value),
    );
  });
</script>

<template>
  <CSelect
    v-model:value="filterValue"
    class="chorus-bucket-filter"
    multiple
    filterable
    clearable
    :placeholder="placeholder"
    :options="bucketOptions"
    :max-tag-count="1"
  >
    <template #empty>
      <CResult
        type="empty"
        size="tiny"
        v-if="!bucketOptions.length"
      >
        <template #title>
          {{ noResultTitleValue }}
        </template>

        <p>{{ noResultTextValue }}</p>
      </CResult>
    </template>
  </CSelect>
</template>
