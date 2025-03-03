import { storeToRefs } from 'pinia';
import { useRouter } from 'vue-router';
import { RouteName } from '@/utils/types/router';
import { useChorusReplicationsStore } from '@/stores/chorusReplicationsStore';
import { ReplicationStatusFilter } from '@/utils/types/chorus';

export const useReplicationsWidget = () => {
  const { filterStatuses } = storeToRefs(useChorusReplicationsStore());
  const router = useRouter();

  const goToReplicationsPage = () => {
    router.push({ name: RouteName.CHORUS_REPLICATION });
  };

  const showActiveReplications = () => {
    filterStatuses.value = [ReplicationStatusFilter.ACTIVE];
    goToReplicationsPage();
  };

  const showPausedReplications = () => {
    filterStatuses.value = [ReplicationStatusFilter.PAUSED];
    goToReplicationsPage();
  };

  const showUpToDateReplications = () => {
    filterStatuses.value = [ReplicationStatusFilter.LIVE_UP_TO_DATE];
    router.push({ name: RouteName.CHORUS_REPLICATION });
  };

  const showInProgressReplications = () => {
    filterStatuses.value = [ReplicationStatusFilter.INITIAL_IN_PROGRESS];
    router.push({ name: RouteName.CHORUS_REPLICATION });
  };

  const showBehindReplications = () => {
    filterStatuses.value = [ReplicationStatusFilter.LIVE_BEHIND];
    router.push({ name: RouteName.CHORUS_REPLICATION });
  };

  return {
    goToReplicationsPage,
    showActiveReplications,
    showPausedReplications,
    showUpToDateReplications,
    showInProgressReplications,
    showBehindReplications,
  };
};
