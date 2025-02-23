<script setup lang="ts">
  import {
    CDescriptionItem,
    CDescriptionList,
    CResult,
    CTag,
    CTile,
    I18nLocale,
    CSkeleton,
  } from '@clyso/clyso-ui-kit';
  import { useI18n } from 'vue-i18n';
  import { computed, ref } from 'vue';
  import { useRouter } from 'vue-router';
  import { storeToRefs } from 'pinia';
  import type { ChorusReplication } from '@/utils/types/chorus';
  import { ReplicationStatusFilter } from '@/utils/types/chorus';
  import { ChorusService } from '@/services/ChorusService';
  import { ReplicationsHelper } from '@/utils/helpers/ReplicationsHelper';
  import { RouteName } from '@/utils/types/router';
  import { useChorusReplicationsStore } from '@/stores/chorusReplicationsStore';

  const { t } = useI18n({
    messages: {
      [I18nLocale.EN]: {
        replicationsTitle: 'Replications',
        total: 'Total',
        status: 'Status',
        progress: 'Progress',
        paused: 'Paused',
        active: 'Active',
        inProgress: 'In progress',
        upToDate: 'Up to date',
        behind: 'Behind',
        errorMessage: 'An error occurred while getting the replication data.',
      },
      [I18nLocale.DE]: {
        replicationsTitle: 'Replications',
        total: 'Total',
        status: 'Status',
        progress: 'Progress',
        paused: 'Paused',
        active: 'Active',
        inProgress: 'In progress',
        upToDate: 'Up to date',
        behind: 'Behind',
        errorMessage: 'An error occurred while getting the replication data.',
      },
    },
  });

  withDefaults(
    defineProps<{
      isInitializing?: boolean;
    }>(),
    {
      isInitializing: false,
    },
  );

  const emit = defineEmits<{
    (e: 'init', value: boolean): void;
  }>();

  const replications = ref<ChorusReplication[]>([]);
  const hasError = ref<boolean>(false);
  const isLoading = ref<boolean>(false);

  async function getReplications() {
    isLoading.value = true;
    hasError.value = false;

    try {
      const res = await ChorusService.getReplications();

      replications.value = res.replications;
    } catch {
      hasError.value = true;
    } finally {
      isLoading.value = false;
    }
  }

  async function initReplications() {
    emit('init', true);

    try {
      await getReplications();
    } finally {
      emit('init', false);
    }
  }

  initReplications();

  const router = useRouter();

  const { filterStatuses } = storeToRefs(useChorusReplicationsStore());

  const count = computed<number>(() => replications.value.length);
  const pausedCount = computed<number>(
    () => replications.value.filter((r) => r.isPaused).length,
  );
  const activeCount = computed<number>(() => count.value - pausedCount.value);
  const inProgressCount = computed<number>(
    () => replications.value.filter((r) => !r.isInitDone).length,
  );
  const upToDateCount = computed<number>(
    () =>
      replications.value.filter(
        (r) => r.isInitDone && !ReplicationsHelper.isLiveReplicationBehind(r),
      ).length,
  );
  const behindCount = computed<number>(
    () =>
      replications.value.filter(
        (r) => r.isInitDone && ReplicationsHelper.isLiveReplicationBehind(r),
      ).length,
  );

  function navigate(
    tag: 'total' | 'active' | 'paused' | 'upToDate' | 'inProgress' | 'behind',
  ) {
    if (tag === 'total') {
      router.push({ name: RouteName.CHORUS_REPLICATION });

      return;
    }

    if (tag === 'active' && activeCount) {
      filterStatuses.value = [ReplicationStatusFilter.ACTIVE];
      router.push({ name: RouteName.CHORUS_REPLICATION });

      return;
    }

    if (tag === 'paused' && pausedCount.value) {
      filterStatuses.value = [ReplicationStatusFilter.PAUSED];
      router.push({ name: RouteName.CHORUS_REPLICATION });

      return;
    }

    if (tag === 'inProgress' && inProgressCount.value) {
      filterStatuses.value = [ReplicationStatusFilter.INITIAL_IN_PROGRESS];
      router.push({ name: RouteName.CHORUS_REPLICATION });

      return;
    }

    if (tag === 'upToDate' && upToDateCount.value) {
      filterStatuses.value = [ReplicationStatusFilter.LIVE_UP_TO_DATE];
      router.push({ name: RouteName.CHORUS_REPLICATION });

      return;
    }

    if (tag === 'behind' && behindCount.value) {
      filterStatuses.value = [ReplicationStatusFilter.LIVE_BEHIND];
      router.push({ name: RouteName.CHORUS_REPLICATION });
    }
  }
</script>

<template>
  <CTile
    class="replications-widget"
    :is-loading="isLoading || isInitializing"
  >
    <template #title>
      {{ t('replicationsTitle') }}
    </template>

    <div class="replications-widget__content">
      <CResult
        v-if="hasError"
        key="error"
        type="error"
        size="tiny"
        class="replications-widget__error-result"
        @positive-click="getReplications"
      >
        <template #title>
          {{ t('errorTitle') }}
        </template>
        {{ t('errorMessage') }}
      </CResult>

      <div
        v-else
        key="content"
      >
        <CDescriptionList
          class="replications-info"
          label-placement="left"
          :columns="1"
        >
          <CDescriptionItem class="replications-info__item">
            <template #label> {{ t('total') }}: </template>

            <CTag
              class="replication-status-tag"
              :class="{ 'replication-status-tag--clickable': count !== 0 }"
              :bordered="false"
              round
              tabindex="0"
              @click="navigate('total')"
              @keydown.enter="navigate('total')"
            >
              {{ count }}
            </CTag>
          </CDescriptionItem>
          <CDescriptionItem class="replications-info__item">
            <template #label> {{ t('status') }}: </template>

            <div class="replications-info__tag-list">
              <CTag
                class="replication-status-tag"
                :class="{
                  'replication-status-tag--clickable': activeCount !== 0,
                }"
                :bordered="false"
                type="info"
                tabindex="0"
                @click="navigate('active')"
                @keydown.enter="navigate('active')"
              >
                {{ t('active') }}: <strong>{{ activeCount }}</strong>
              </CTag>
              <CTag
                class="replication-status-tag"
                :class="{
                  'replication-status-tag--clickable': pausedCount !== 0,
                }"
                :bordered="false"
                type="warning"
                tabindex="0"
                @click="navigate('paused')"
                @keydown.enter="navigate('paused')"
              >
                {{ t('paused') }}: <strong>{{ pausedCount }}</strong>
              </CTag>
            </div>
          </CDescriptionItem>
          <CDescriptionItem class="replications-info__item">
            <template #label> {{ t('progress') }}: </template>

            <div class="replications-info__tag-list">
              <CTag
                class="replication-status-tag"
                :class="{
                  'replication-status-tag--clickable': upToDateCount !== 0,
                }"
                :bordered="false"
                type="success"
                round
                tabindex="0"
                @click="navigate('upToDate')"
                @keydown.enter="navigate('upToDate')"
              >
                {{ t('upToDate') }}: <strong>{{ upToDateCount }}</strong>
              </CTag>
              <CTag
                class="replication-status-tag"
                :class="{
                  'replication-status-tag--clickable': inProgressCount !== 0,
                }"
                :bordered="false"
                type="info"
                round
                tabindex="0"
                @click="navigate('inProgress')"
                @keydown.enter="navigate('inProgress')"
              >
                {{ t('inProgress') }}: <strong>{{ inProgressCount }}</strong>
              </CTag>
              <CTag
                class="replication-status-tag"
                :class="{
                  'replication-status-tag--clickable': behindCount !== 0,
                }"
                :bordered="false"
                type="warning"
                round
                tabindex="0"
                @click="navigate('behind')"
                @keydown.enter="navigate('behind')"
              >
                {{ t('behind') }}: <strong>{{ behindCount }}</strong>
              </CTag>
            </div>
          </CDescriptionItem>
        </CDescriptionList>
      </div>
    </div>

    <template #loading-content>
      <CSkeleton
        type="text"
        :repeat="4"
      />
      <CSkeleton
        type="text"
        width="60%"
      />
    </template>
  </CTile>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .replications-widget {
    &__skeleton {
      @include utils.absolute-fit;
    }
  }

  .replications-info {
    &__item {
      align-items: baseline;
    }

    &__tag-list {
      display: flex;
      align-items: baseline;
      flex-wrap: wrap;
      gap: utils.unit(2);
    }
  }

  .replication-status-tag {
    &--clickable {
      transition: opacity utils.$transition-duration;
      cursor: pointer;

      ::v-deep(.c-icon) {
        line-height: 14px;
      }

      &:hover {
        opacity: 0.8;
      }

      &:active {
        opacity: 0.9;
      }
    }
  }
</style>
