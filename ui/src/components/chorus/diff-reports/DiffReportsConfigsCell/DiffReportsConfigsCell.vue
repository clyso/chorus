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
  import { useI18n } from 'vue-i18n';
  import {
    CDescriptionItem,
    CDescriptionList,
    CTag,
    CIcon,
  } from '@clyso/clyso-ui-kit';
  import { computed } from 'vue';
  import { IconName } from '@/utils/types/icon';
  import i18nDiffReports from '@/components/chorus/diff-reports/i18nDiffReports';
  import type { DiffReport } from '@/utils/types/chorus';

  const props = defineProps<{
    report: DiffReport;
  }>();

  const { t } = useI18n({
    messages: i18nDiffReports,
  });

  const isVersioned = computed<boolean>(() => props.report.versioned);
  const isIgnoreEtags = computed<boolean>(() => props.report.ignoreEtags);
  const isIgnoreSize = computed<boolean>(() => props.report.ignoreSizes);
</script>

<template>
  <div class="diff-reports-configs-cell">
    <CDescriptionList
      class="reports-list"
      label-placement="left"
      :columns="1"
      size="small"
    >
      <CDescriptionItem
        class="reports-list__item reports-list__item--versioned"
      >
        <template #label> {{ t('columnVersioned') }}: </template>

        <CTag
          :bordered="false"
          round
          size="small"
          :type="isVersioned ? 'success' : 'warning'"
        >
          <template #icon>
            <CIcon
              :is-inline="true"
              :name="
                isVersioned
                  ? IconName.BASE_CHECKMARK
                  : IconName.BASE_CLOSE_CIRCLE
              "
            />
          </template>
          {{ isVersioned ? t('configVersioned') : t('configNotVersioned') }}
        </CTag>
      </CDescriptionItem>

      <CDescriptionItem
        class="reports-list__item reports-list__item--ignores-etags"
      >
        <template #label> {{ t('columnEtags') }}: </template>

        <CTag
          :bordered="false"
          round
          size="small"
          :type="isIgnoreEtags ? 'warning' : 'success'"
        >
          <template #icon>
            <CIcon
              :is-inline="true"
              :name="
                isIgnoreEtags
                  ? IconName.BASE_CLOSE_CIRCLE
                  : IconName.BASE_CHECKMARK
              "
            />
          </template>
          {{
            isIgnoreEtags ? t('configIgnoresEtags') : t('configNotIgnoresEtags')
          }}
        </CTag>
      </CDescriptionItem>

      <CDescriptionItem
        class="reports-list__item reports-list__item--ignores-sizes"
      >
        <template #label> {{ t('columnSizes') }}: </template>

        <CTag
          :bordered="false"
          round
          size="small"
          :type="isIgnoreSize ? 'warning' : 'success'"
        >
          <template #icon>
            <CIcon
              :is-inline="true"
              :name="
                isIgnoreSize
                  ? IconName.BASE_CLOSE_CIRCLE
                  : IconName.BASE_CHECKMARK
              "
            />
          </template>
          {{
            isIgnoreSize ? t('configIgnoresSizes') : t('configNotIgnoresSizes')
          }}
        </CTag>
      </CDescriptionItem>
    </CDescriptionList>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .reports-list {
    gap: utils.unit(1);

    &__item {
      align-items: center;
    }

    :deep(.c-description-item__label) {
      font-weight: 400;
      white-space: nowrap;
    }

    :deep(.c-icon) {
      width: 12px;
      height: 12px;
    }
  }
</style>
