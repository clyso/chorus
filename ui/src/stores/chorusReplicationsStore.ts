import { defineStore } from 'pinia';
import { computed, h, reactive, toRefs } from 'vue';
import type {
  DataTablePaginationObject,
  DataTableSortState,
} from '@clyso/clyso-ui-kit';
import { useNotification } from '@clyso/clyso-ui-kit';
import { useI18n } from 'vue-i18n';
import type {
  ChorusReplication,
  ChorusUserReplication,
} from '@/utils/types/chorus';
import { ReplicationStatusFilter } from '@/utils/types/chorus';
import { GeneralHelper } from '@/utils/helpers/GeneralHelper';
import type { AddId } from '@/utils/types/helper';
import { ChorusService } from '@/services/ChorusService';
import ReplicationsShortList from '@/components/chorus/replications/ReplicationsShortList/ReplicationsShortList.vue';
import i18nReplications from '@/components/chorus/replications/i18nReplications';
import { ReplicationsHelper } from '@/utils/helpers/ReplicationsHelper';

interface ChorusReplicationsState {
  isLoading: boolean;
  hasError: boolean;
  replications: AddId<ChorusReplication>[];
  userReplications: ChorusUserReplication[];
  userReplicationToDelete: {
    replication: ChorusUserReplication;
    isProcessing: boolean;
    isConfirmationShown: boolean;
    isBucketsDeletion: boolean;
    linkedBucketReplications: AddId<ChorusReplication>[];
    hasError: boolean;
  } | null;
  sorter: DataTableSortState | null;
  page: number;
  pageSize: number;
  pollingRequest: Promise<unknown> | null;
  pollingTimeout: number | null;

  selectedReplicationIds: string[];
  isResumeSelectedProcessing: boolean;
  isPauseSelectedProcessing: boolean;
  isDeleteSelectedProcessing: boolean;

  filterUsers: string[];
  filterBucket: string;
  filterToStorages: string[];
  filterStatuses: ReplicationStatusFilter[];
  filterCreatedAtRange: [number, number] | null;
}

const PAGE_SIZES = [10, 20, 30, 50, 100];

function getChorusReplicationId(replication: ChorusReplication) {
  return `${replication.user}${replication.bucket}${replication.from}${replication.to}`;
}

function getInitialState(): ChorusReplicationsState {
  return {
    isLoading: false,
    hasError: false,
    replications: [],
    userReplications: [],
    userReplicationToDelete: null,
    sorter: null,
    page: 1,
    pageSize: PAGE_SIZES[0],
    pollingRequest: null,
    pollingTimeout: null,
    selectedReplicationIds: [],
    isResumeSelectedProcessing: false,
    isPauseSelectedProcessing: false,
    isDeleteSelectedProcessing: false,

    filterUsers: [],
    filterBucket: '',
    filterToStorages: [],
    filterStatuses: [],
    filterCreatedAtRange: null,
  };
}

export const useChorusReplicationsStore = defineStore(
  'chorusReplications',
  () => {
    const state = reactive<ChorusReplicationsState>(getInitialState());

    const { createNotification, removeNotification } = useNotification();

    const { t } = useI18n({
      messages: i18nReplications,
    });

    const hasNoData = computed<boolean>(() => state.replications.length === 0);

    const filteredReplications = computed<AddId<ChorusReplication>[]>(() =>
      state.replications.filter((replication) => {
        const isUserMatched =
          !state.filterUsers.length ||
          state.filterUsers.includes(replication.user);
        const isBucketMatched =
          !state.filterBucket ||
          replication.bucket
            .toLowerCase()
            .trim()
            .includes(state.filterBucket.toLowerCase().trim());
        const isToStorageMatched =
          !state.filterToStorages.length ||
          state.filterToStorages.includes(replication.to);
        const isStatusMatched =
          !state.filterStatuses.length ||
          state.filterStatuses.every((status) =>
            ReplicationsHelper.isReplicationStatusMatched(replication, status),
          );
        const isCreatedAtMatched =
          !state.filterCreatedAtRange ||
          ReplicationsHelper.isReplicationCreateAtMatched(
            replication,
            state.filterCreatedAtRange,
          );

        return (
          isUserMatched &&
          isBucketMatched &&
          isToStorageMatched &&
          isStatusMatched &&
          isCreatedAtMatched
        );
      }),
    );

    const isFiltered = computed<boolean>(
      () =>
        state.filterUsers.length !== 0 ||
        state.filterBucket !== '' ||
        state.filterToStorages.length !== 0 ||
        state.filterStatuses.length !== 0 ||
        state.filterCreatedAtRange !== null,
    );

    function clearFilters() {
      state.filterUsers = [];
      state.filterBucket = '';
      state.filterToStorages = [];
      state.filterStatuses = [];
      state.filterCreatedAtRange = null;
    }

    const computedReplications = computed<AddId<ChorusReplication>[]>(() => {
      const pageReplications = state.sorter
        ? GeneralHelper.orderBy(
            filteredReplications.value,
            [state.sorter.columnKey],
            [state.sorter.order === 'ascend' ? 'asc' : 'desc'],
          )
        : filteredReplications.value;

      const start = (state.page - 1) * state.pageSize;
      const end = state.page * state.pageSize;

      return pageReplications.slice(start, end);
    });

    const pagination = computed<DataTablePaginationObject>(() => ({
      page: state.page,
      pageSize: state.pageSize,
      showSizePicker: true,
      pageSizes: PAGE_SIZES,
      pageCount: Math.ceil(filteredReplications.value.length / state.pageSize),
      itemCount: filteredReplications.value.length,
      prefix({ itemCount }) {
        if (state.isLoading || state.hasError) {
          return '';
        }

        if (isFiltered.value) {
          return `Filtered: ${filteredReplications.value.length} / Total: ${state.replications.length}`;
        }

        return `Total: ${itemCount}`;
      },
    }));

    async function getReplications() {
      const res = await ChorusService.getReplications();

      state.replications = res.replications.map((replication) => ({
        ...replication,
        id: getChorusReplicationId(replication),
      }));
    }

    async function getUserReplications() {
      const res = await ChorusService.getUserReplications();

      state.userReplications = res.replications;
    }

    async function startReplicationPolling() {
      try {
        await stopReplicationPolling();

        state.pollingRequest = Promise.all([
          getReplications(),
          getUserReplications(),
        ]);

        await state.pollingRequest;
      } finally {
        state.pollingRequest = null;
        state.pollingTimeout = window.setTimeout(startReplicationPolling, 5000);
      }
    }

    async function stopReplicationPolling() {
      let error: Error | null = null;

      if (state.pollingRequest) {
        try {
          await state.pollingRequest;
        } catch (e) {
          error = e as Error;
        } finally {
          state.pollingRequest = null;
        }
      }

      if (!state.pollingTimeout) {
        return;
      }

      clearTimeout(state.pollingTimeout);
      state.pollingTimeout = null;

      if (error) {
        throw error;
      }
    }

    async function initReplicationsPage() {
      state.isLoading = true;

      try {
        await startReplicationPolling();

        state.hasError = false;
      } catch {
        state.hasError = true;
        await stopReplicationPolling();
      } finally {
        state.isLoading = false;
      }
    }

    function setReplicationById(
      id: string | number,
      partialReplication: Partial<AddId<ChorusReplication>>,
    ) {
      const index = state.replications.findIndex(
        (replication) => replication.id === id,
      );

      if (index === -1) {
        return;
      }

      state.replications.splice(index, 1, {
        ...state.replications[index],
        ...partialReplication,
      });
    }

    async function setReplicationPaused(
      replication: AddId<ChorusReplication>,
      isPaused: boolean,
    ) {
      const replicationIndex = state.replications.findIndex(
        ({ id }) => replication.id === id,
      );

      if (replicationIndex === -1) {
        return;
      }

      const { user, bucket, from, to } = replication;

      await stopReplicationPolling();
      await (
        isPaused
          ? ChorusService.pauseBucketReplication
          : ChorusService.resumeBucketReplication
      )({
        user,
        bucket,
        from,
        to,
      });

      state.replications.splice(replicationIndex, 1, {
        ...replication,
        isPaused,
      });
      startReplicationPolling();
    }

    async function deleteReplication(replication: AddId<ChorusReplication>) {
      let replicationIndex = state.replications.findIndex(
        ({ id }) => replication.id === id,
      );

      if (replicationIndex === -1) {
        return;
      }

      const { user, bucket, from, to } = replication;

      await stopReplicationPolling();
      await ChorusService.deleteBucketReplication({
        user,
        bucket,
        from,
        to,
      });

      replicationIndex = state.replications.findIndex(
        ({ id }) => replication.id === id,
      );

      state.replications.splice(replicationIndex, 1);
      startReplicationPolling();
    }

    function setUserReplicationToDelete(
      userReplication: ChorusUserReplication | null,
    ) {
      if (!userReplication) {
        state.userReplicationToDelete = null;

        return;
      }

      const linkedBucketReplications = state.replications.filter(
        (replication) =>
          replication.user === userReplication.user &&
          replication.to === userReplication.to &&
          replication.from === userReplication.from,
      );

      state.userReplicationToDelete = {
        replication: userReplication,
        isProcessing: false,
        isConfirmationShown: true,
        isBucketsDeletion: false,
        hasError: false,
        linkedBucketReplications,
      };
    }

    function setUserReplicationToDeleteProcessing(isProcessing: boolean) {
      if (!state.userReplicationToDelete) {
        return;
      }

      state.userReplicationToDelete.isProcessing = isProcessing;
    }

    function setUserReplicationToDeleteConfirmation(isShown: boolean) {
      if (!state.userReplicationToDelete) {
        return;
      }

      state.userReplicationToDelete.isConfirmationShown = isShown;
    }

    async function deleteUserReplication() {
      const userReplication = state.userReplicationToDelete?.replication;

      if (!userReplication || !state.userReplicationToDelete) {
        return;
      }

      setUserReplicationToDeleteProcessing(true);
      state.userReplicationToDelete.hasError = false;

      try {
        await stopReplicationPolling();
        await ChorusService.deleteUserReplication({
          ...state.userReplicationToDelete.replication,
          deleteBucketReplications:
            state.userReplicationToDelete.isBucketsDeletion,
        });

        state.userReplications = state.userReplications.filter(
          (replication) =>
            replication.user !== userReplication.user ||
            replication.to !== userReplication.to ||
            replication.from !== userReplication.from,
        );

        const { isBucketsDeletion, linkedBucketReplications } =
          state.userReplicationToDelete;

        if (isBucketsDeletion) {
          state.replications = state.replications.filter(
            (replication) => !linkedBucketReplications.includes(replication),
          );
          state.page = 1;
        }

        createNotification({
          type: 'success',
          title: t('deleteUserSuccessTitle'),
          duration: 4000,
          content: () =>
            h('div', [
              t('deleteUserSuccessContent'),
              h('strong', { style: 'font-weight: 600' }, [
                userReplication.user,
              ]),
              isBucketsDeletion
                ? h(
                    'div',
                    {
                      style: 'margin-top: 8px;',
                    },
                    [
                      t('deleteUserSuccessContentBuckets', {
                        total: linkedBucketReplications.length,
                      }),
                      h(ReplicationsShortList, {
                        replications: linkedBucketReplications,
                      }),
                    ],
                  )
                : '',
            ]),
        });

        startReplicationPolling();
      } catch (e) {
        state.userReplicationToDelete.hasError = true;
        const errorNotification = createNotification({
          type: 'error',
          title: t('deleteUserErrorTitle'),
          positiveText: t('deleteUserErrorAction'),
          positiveHandler: async () => {
            removeNotification(errorNotification.value.id);
            await deleteUserReplication();
            setUserReplicationToDelete(null);
          },
          content: () =>
            h('div', [
              t('deleteUserErrorContent'),
              h('strong', { style: 'font-weight: 600' }, [
                userReplication.user,
              ]),
            ]),
        });

        throw e;
      } finally {
        setUserReplicationToDeleteProcessing(false);
      }
    }

    const selectedReplicationsCount = computed(
      () => state.selectedReplicationIds.length,
    );
    const isAnyReplicationsSelected = computed(
      () => state.selectedReplicationIds.length !== 0,
    );
    const selectedReplications = computed<AddId<ChorusReplication>[]>(() =>
      state.replications.filter((replication) =>
        state.selectedReplicationIds.includes(String(replication.id)),
      ),
    );

    const isSelectedProcessing = computed(
      () =>
        state.isResumeSelectedProcessing ||
        state.isPauseSelectedProcessing ||
        state.isDeleteSelectedProcessing,
    );

    async function resumeReplications(
      replications: AddId<ChorusReplication>[],
    ) {
      state.isResumeSelectedProcessing = true;

      const successList: AddId<ChorusReplication>[] = [];
      const errorList: AddId<ChorusReplication>[] = [];

      await Promise.all(
        replications.map(async (replication) => {
          const { from, to, user, bucket } = replication;

          try {
            await ChorusService.resumeBucketReplication({
              from,
              to,
              user,
              bucket,
            });
            successList.push(replication);
          } catch {
            errorList.push(replication);
          }
        }),
      );

      if (errorList.length !== 0) {
        const errorNotification = createNotification({
          type: 'error',
          title: t('resumeErrorTitle'),
          positiveText: t('errorAction'),
          positiveHandler: () => {
            removeNotification(errorNotification.value.id);
            resumeReplications(errorList);
          },
          content: () =>
            h('div', [
              t('resumeSelectedErrorContent', { total: errorList.length }),
              h(ReplicationsShortList, {
                replications: errorList,
              }),
            ]),
        });
      }

      if (successList.length !== 0) {
        successList.forEach((replication) => {
          setReplicationById(replication.id, { isPaused: false });
        });
        createNotification({
          type: 'success',
          title: t('resumeSuccessTitle'),
          duration: 4000,
          content: () =>
            h('div', [
              t('resumeSelectedSuccessContent', { total: successList.length }),
              h(ReplicationsShortList, {
                replications: successList,
              }),
            ]),
        });
      }

      startReplicationPolling();
      state.isResumeSelectedProcessing = false;
    }

    async function pauseReplications(replications: AddId<ChorusReplication>[]) {
      state.isPauseSelectedProcessing = true;

      const successList: AddId<ChorusReplication>[] = [];
      const errorList: AddId<ChorusReplication>[] = [];

      await Promise.all(
        replications.map(async (replication) => {
          const { from, to, user, bucket } = replication;

          try {
            await ChorusService.pauseBucketReplication({
              from,
              to,
              user,
              bucket,
            });
            successList.push(replication);
          } catch {
            errorList.push(replication);
          }
        }),
      );

      if (errorList.length !== 0) {
        const errorNotification = createNotification({
          type: 'error',
          title: t('pauseErrorTitle'),
          positiveText: t('errorAction'),
          positiveHandler: () => {
            removeNotification(errorNotification.value.id);
            pauseReplications(errorList);
          },
          content: () =>
            h('div', [
              t('pauseSelectedErrorContent', { total: errorList.length }),
              h(ReplicationsShortList, {
                replications: errorList,
              }),
            ]),
        });
      }

      if (successList.length !== 0) {
        successList.forEach((replication) => {
          setReplicationById(replication.id, { isPaused: true });
        });
        createNotification({
          type: 'success',
          title: t('pauseSuccessTitle'),
          duration: 4000,
          content: () =>
            h('div', [
              t('pauseSelectedSuccessContent', { total: successList.length }),
              h(ReplicationsShortList, {
                replications: successList,
              }),
            ]),
        });
      }

      startReplicationPolling();
      state.isPauseSelectedProcessing = false;
    }

    async function deleteReplications(
      replications: AddId<ChorusReplication>[],
    ) {
      state.isDeleteSelectedProcessing = true;

      const successList: AddId<ChorusReplication>[] = [];
      const errorList: AddId<ChorusReplication>[] = [];

      await Promise.all(
        replications.map(async (replication) => {
          const { from, to, user, bucket } = replication;

          try {
            await ChorusService.deleteBucketReplication({
              from,
              to,
              user,
              bucket,
            });
            successList.push(replication);
          } catch {
            errorList.push(replication);
          }
        }),
      );

      if (errorList.length !== 0) {
        const errorNotification = createNotification({
          type: 'error',
          title: t('deleteErrorTitle'),
          positiveText: t('errorAction'),
          positiveHandler: () => {
            removeNotification(errorNotification.value.id);
            deleteReplications(errorList);
          },
          content: () =>
            h('div', [
              t('deleteSelectedErrorContent', { total: errorList.length }),
              h(ReplicationsShortList, {
                replications: errorList,
              }),
            ]),
        });
      }

      if (successList.length !== 0) {
        const successListIds = successList.map((item) => item.id);

        state.replications = state.replications.filter(
          (item) => !successListIds.includes(item.id),
        );
        state.selectedReplicationIds = state.selectedReplicationIds.filter(
          (item) => !successListIds.includes(item),
        );

        const pageCount = pagination.value.pageCount ?? 1;

        if (state.page > pageCount) {
          state.page = pageCount;
        }

        createNotification({
          type: 'success',
          title: t('deleteSuccessTitle'),
          duration: 4000,
          content: () =>
            h('div', [
              t('deleteSelectedSuccessContent', { total: successList.length }),
              h(ReplicationsShortList, {
                replications: successList,
              }),
            ]),
        });
      }

      startReplicationPolling();
      state.isDeleteSelectedProcessing = false;
    }

    async function $reset() {
      try {
        await stopReplicationPolling();
      } finally {
        Object.assign(state, getInitialState());
      }
    }

    return {
      ...toRefs(state),
      hasNoData,
      pagination,
      computedReplications,
      initReplicationsPage,
      setReplicationPaused,
      deleteReplication,
      setUserReplicationToDelete,
      setUserReplicationToDeleteProcessing,
      setUserReplicationToDeleteConfirmation,
      deleteUserReplication,
      selectedReplicationsCount,
      isAnyReplicationsSelected,
      selectedReplications,
      isSelectedProcessing,
      resumeReplications,
      pauseReplications,
      deleteReplications,
      isFiltered,
      clearFilters,
      $reset,
    };
  },
);
