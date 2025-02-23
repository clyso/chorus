import type { AddId } from '@/utils/types/helper';
import type { ChorusReplication } from '@/utils/types/chorus';
import { ReplicationStatusFilter } from '@/utils/types/chorus';

export abstract class ReplicationsHelper {
  static getLiveReplicationEventsDifference(
    replication: ChorusReplication,
  ): number {
    const { events, eventsDone } = replication;

    if (!events) {
      return 0;
    }

    return Number(events) - Number(eventsDone);
  }

  static isLiveReplicationBehind(replication: ChorusReplication): boolean {
    return this.getLiveReplicationEventsDifference(replication) > 0;
  }

  static isReplicationStatusMatched(
    replication: AddId<ChorusReplication>,
    status: ReplicationStatusFilter | null,
  ): boolean {
    if (status === ReplicationStatusFilter.ACTIVE) {
      return !replication.isPaused;
    }

    if (status === ReplicationStatusFilter.PAUSED) {
      return replication.isPaused;
    }

    if (status === ReplicationStatusFilter.INITIAL_IN_PROGRESS) {
      return !replication.isInitDone;
    }

    if (status === ReplicationStatusFilter.INITIAL_DONE) {
      return replication.isInitDone;
    }

    const isLiveBehind = this.isLiveReplicationBehind(replication);

    if (status === ReplicationStatusFilter.LIVE_UP_TO_DATE) {
      return !isLiveBehind && replication.isInitDone;
    }

    if (status === ReplicationStatusFilter.LIVE_BEHIND) {
      return isLiveBehind && replication.isInitDone;
    }

    return false;
  }

  static isReplicationCreateAtMatched(
    replication: AddId<ChorusReplication>,
    [rangeStartMs, rangeEndMs]: [number, number],
  ): boolean {
    const createdAtMs = new Date(replication.createdAt).getTime();

    return createdAtMs >= rangeStartMs && createdAtMs <= rangeEndMs;
  }
}
