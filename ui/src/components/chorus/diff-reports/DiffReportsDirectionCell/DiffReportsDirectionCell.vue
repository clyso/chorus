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
  import i18nDiffReports from '@/components/chorus/diff-reports/i18nDiffReports';
  import type { DiffReportLocation } from '@/utils/types/chorus';
  import ChorusDirectionCell from '@/components/chorus/common/ChorusDirectionCell/ChorusDirectionCell.vue';

  defineProps<{
    locations: [DiffReportLocation, DiffReportLocation];
  }>();

  const { t } = useI18n({
    messages: i18nDiffReports,
  });
</script>

<template>
  <ChorusDirectionCell
    :from-text="locations[0].storage"
    :to-text="locations[1].storage"
    :from-tooltip="t('diffReportFrom')"
    :to-tooltip="t('diffReportTo')"
    to-type="warning"
  />
</template>
