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
  import {
    CButton,
    CIcon,
    I18nLocale,
    CAspectRatio,
    CSkeleton,
  } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import { computed, onBeforeMount, ref } from 'vue';
  import { type ChorusReplication } from '@/utils/types/chorus';
  import { ChorusService } from '@/services/ChorusService';
  import { IconName } from '@/utils/types/icon';
  import { RouteName } from '@/utils/types/router';
  import HomeWidget from '@/components/chorus/common/HomeWidget/HomeWidget.vue';
  import HomeStatistics from '@/components/chorus/common/HomeStatistics/HomeStatistics.vue';
  import HomeWidgetAction from '@/components/chorus/common/HomeWidgetAction/HomeWidgetAction.vue';
  import { useReplicationsWidget } from '@/components/chorus/home/ReplicationsWidget/useReplicationsWidget';
  import ReplicationsWidgetStatusChart from '@/components/chorus/home/ReplicationsWidget/ReplicationsWidgetStatusChart.vue';
  import ReplicationsWidgetProgressChart from '@/components/chorus/home/ReplicationsWidget/ReplicationsWidgetProgressChart.vue';
  import { ReplicationsHelper } from '@/utils/helpers/ReplicationsHelper';

  const { t } = useI18n({
    messages: {
      [I18nLocale.EN]: {
        replicationsTitle: 'Replications',
        replicationsGoToList: 'Go to replications',
        replicationsAdd: 'Add Replication',
        total: 'Total',
        status: 'Status',
        progress: 'Progress',
        paused: 'Paused',
        active: 'Active',
        inProgress: 'In progress',
        upToDate: 'Up to date',
        behind: 'Behind',
        errorMessage: 'An error occurred while getting the replication data.',
        replicationsEmptyTitle: 'No Replications Available',
        replicationsEmptyText:
          ' Set up replication to automatically sync objects across buckets and keep your data up to date.',
        replicationsEmptyButton: 'Add Replication',
      },
      [I18nLocale.DE]: {
        replicationsTitle: 'Replikationen',
        replicationsGoToList: 'Zu dem Replikationen',
        replicationsAdd: 'Replikation hinzufügen',
        total: 'Gesamt',
        status: 'Status',
        progress: 'Fortschritt',
        paused: 'Pausiert',
        active: 'Aktiv',
        inProgress: 'In Bearbeitung',
        upToDate: 'Aktuell',
        behind: 'Hinterher',
        errorMessage:
          'Beim Abrufen der Replikationsdaten ist ein Fehler aufgetreten.',
        replicationsEmptyTitle: 'Keine Replikationen verfügbar',
        replicationsEmptyText:
          'Richten Sie eine Replikation ein, um Objekte automatisch zwischen Buckets zu synchronisieren und Ihre Daten auf dem neuesten Stand zu halten.',
        replicationsEmptyButton: 'Replikation hinzufügen',
      },
    },
  });

  withDefaults(
    defineProps<{
      isPageLoading?: boolean;
    }>(),
    {
      isPageLoading: false,
    },
  );

  const emit = defineEmits<{
    (e: 'loading', value: boolean): void;
  }>();

  const replications = ref<ChorusReplication[]>([]);
  const hasError = ref<boolean>(false);
  const isLoading = ref<boolean>(false);

  async function getReplications() {
    hasError.value = false;

    try {
      const res = await ChorusService.getReplications();

      replications.value = res?.replications ?? [];
    } catch {
      hasError.value = true;
    }
  }

  const POLL_TIMEOUT = 5000;

  async function startReplicationsPolling() {
    await getReplications();

    if (hasError.value) {
      return;
    }

    setTimeout(startReplicationsPolling, POLL_TIMEOUT);
  }

  async function initReplications() {
    emit('loading', true);
    isLoading.value = true;

    try {
      await startReplicationsPolling();
    } finally {
      emit('loading', false);
      isLoading.value = false;
    }
  }

  onBeforeMount(initReplications);

  const {
    goToReplicationsPage,
    showActiveReplications,
    showBehindReplications,
    showPausedReplications,
    showInProgressReplications,
    showUpToDateReplications,
  } = useReplicationsWidget();

  const pausedCount = computed(
    () => replications.value.filter(({ isPaused }) => isPaused).length,
  );
  const activeCount = computed(
    () => replications.value.length - pausedCount.value,
  );

  const upToDateCount = computed(
    () =>
      replications.value.filter(
        (replication) =>
          replication.isInitDone &&
          !ReplicationsHelper.isLiveReplicationBehind(replication),
      ).length,
  );
  const inProgressCount = computed(
    () => replications.value.filter(({ isInitDone }) => !isInitDone).length,
  );
  const behindCount = computed(
    () =>
      replications.value.filter(
        (replication) =>
          replication.isInitDone &&
          ReplicationsHelper.isLiveReplicationBehind(replication),
      ).length,
  );
</script>

<template>
  <HomeWidget
    :is-loading="isLoading || isPageLoading"
    :has-error="hasError"
    :is-empty="replications.length === 0"
    :icon-name="IconName.BASE_SWAP_HORIZONTAL"
    class="replications-widget"
    @retry="startReplicationsPolling"
  >
    <template #title>
      {{ t('replicationsTitle') }}
    </template>

    <template #actions>
      <HomeWidgetAction
        :icon-name="IconName.BASE_LIST"
        :to="{ name: RouteName.CHORUS_REPLICATION }"
        :tooltip-text="t('replicationsGoToList')"
      />
      <HomeWidgetAction
        :icon-name="IconName.BASE_ADD"
        :to="{ name: RouteName.CHORUS_ADD_REPLICATION }"
        :tooltip-text="t('replicationsAdd')"
      />
    </template>

    <div class="replications-widget__content chart-block">
      <div
        class="chart-block__statistics-list chart-block__statistics-list--left"
      >
        <HomeStatistics
          class="chart-block__statistics"
          type="primary"
          size="medium"
          align="left"
          :is-clickable="true"
          @click="goToReplicationsPage"
        >
          <template #label>
            {{ t('total') }}
          </template>

          {{ replications.length }}
        </HomeStatistics>
        <HomeStatistics
          class="chart-block__statistics"
          type="info"
          size="medium"
          align="left"
          :is-clickable="true"
          @click="showActiveReplications"
        >
          <template #label>
            {{ t('active') }}
          </template>

          {{ activeCount }}
        </HomeStatistics>
        <HomeStatistics
          class="chart-block__statistics"
          type="warning"
          size="medium"
          align="left"
          :is-clickable="true"
          @click="showPausedReplications"
        >
          <template #label>
            {{ t('paused') }}
          </template>

          {{ pausedCount }}
        </HomeStatistics>
      </div>

      <ReplicationsWidgetStatusChart
        class="chart-block__chart"
        :active-count="activeCount"
        :paused-count="pausedCount"
      />

      <ReplicationsWidgetProgressChart
        class="chart-block__chart"
        :up-to-date-count="upToDateCount"
        :in-progress-count="inProgressCount"
        :behind-count="behindCount"
      />

      <div
        class="chart-block__statistics-list chart-block__statistics-list--right"
      >
        <HomeStatistics
          class="chart-block__statistics"
          type="success"
          size="medium"
          align="right"
          :is-clickable="true"
          @click="showUpToDateReplications"
        >
          <template #label>
            {{ t('upToDate') }}
          </template>

          {{ upToDateCount }}
        </HomeStatistics>
        <HomeStatistics
          class="chart-block__statistics"
          type="info"
          size="medium"
          align="right"
          :is-clickable="true"
          @click="showInProgressReplications"
        >
          <template #label>
            {{ t('inProgress') }}
          </template>

          {{ inProgressCount }}
        </HomeStatistics>
        <HomeStatistics
          class="chart-block__statistics"
          type="warning"
          size="medium"
          align="right"
          :is-clickable="true"
          @click="showBehindReplications"
        >
          <template #label>
            {{ t('behind') }}
          </template>

          {{ behindCount }}
        </HomeStatistics>
      </div>
    </div>

    <template #empty-title>
      {{ t('replicationsEmptyTitle') }}
    </template>
    <template #empty-text>
      {{ t('replicationsEmptyText') }}
    </template>
    <template #empty-actions>
      <RouterLink :to="{ name: RouteName.CHORUS_ADD_REPLICATION }">
        <CButton
          type="primary"
          ghost
          size="medium"
          tag="div"
        >
          <template #icon>
            <CIcon
              :is-inline="true"
              :name="IconName.BASE_ADD"
            />
          </template>
          {{ t('replicationsEmptyButton') }}
        </CButton>
      </RouterLink>
    </template>

    <template #loading-content>
      <div
        class="replications-widget__content chart-block chart-block--loading"
      >
        <div
          class="chart-block__statistics-list chart-block__statistics-list--left"
        >
          <HomeStatistics :is-loading="true" />
          <HomeStatistics :is-loading="true" />
          <HomeStatistics :is-loading="true" />
        </div>
        <CAspectRatio ratio="1:1">
          <CSkeleton
            class="chart-block__chart-skeleton"
            width="100%"
            height="100%"
          />
        </CAspectRatio>
        <CAspectRatio ratio="1:1">
          <CSkeleton
            class="chart-block__chart-skeleton"
            width="100%"
            height="100%"
          />
        </CAspectRatio>
        <div
          class="chart-block__statistics-list chart-block__statistics-list--right"
        >
          <HomeStatistics
            align="right"
            :is-loading="true"
          />
          <HomeStatistics
            align="right"
            :is-loading="true"
          />
          <HomeStatistics
            align="right"
            :is-loading="true"
          />
        </div>
      </div>
    </template>
  </HomeWidget>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .replications-widget {
    grid-column: span 6;

    @media screen and (min-width: utils.$viewport-desktop) and (max-width: 1300px) {
      grid-column: span 4;
    }

    @include utils.touch {
      grid-column: auto;
      width: 100%;
    }

    &__title {
      display: flex;
      align-items: center;
      gap: utils.unit(5);
    }
  }

  .chart-block {
    display: grid;
    grid-template-columns: auto 1fr 1fr auto;
    gap: utils.unit(3);

    &--loading {
      .chart-block__statistics-list {
        min-width: 80px;
      }
    }

    &__statistics-list {
      display: flex;
      flex-direction: column;
      justify-content: space-between;

      &--left {
        padding-right: 12px;
        border-right: 1px solid var(--border-color);
      }

      &--right {
        padding-left: 12px;
        border-left: 1px solid var(--border-color);
      }
    }

    &__chart {
      max-height: 100%;
    }

    &__chart-skeleton {
      @include utils.absolute-fit;

      ::v-deep(.c-skeleton__type-group) {
        height: 100%;
      }
    }
  }
</style>
