import { defineStore } from 'pinia';
import { reactive, toRefs } from 'vue';
import { useRouter } from 'vue-router';
import type { ChorusStorage } from '@/utils/types/chorus';
import { ChorusService } from '@/services/ChorusService';
import { RouteName } from '@/utils/types/router';

interface ChorusStorageDetailsState {
  isLoading: boolean;
  hasError: boolean;
  storage: ChorusStorage | null;
}

function getInitialState(): ChorusStorageDetailsState {
  return {
    isLoading: false,
    hasError: false,
    storage: null,
  };
}

export const useChorusStorageDetailsStore = defineStore(
  'chorusStorageDetails',
  () => {
    const state = reactive<ChorusStorageDetailsState>(getInitialState());
    const router = useRouter();

    async function initStorageDetails(storageName: string) {
      state.isLoading = true;
      state.hasError = false;

      try {
        const { storages: storagesValue } = await ChorusService.getStorages();

        state.storage =
          storagesValue.find(({ name }) => storageName === name) ?? null;

        if (state.storage === null) {
          router.push({ name: RouteName.CHORUS_STORAGES });
        }
      } catch {
        state.hasError = true;
      } finally {
        state.isLoading = false;
      }
    }

    async function $reset() {
      Object.assign(state, getInitialState());
    }

    return {
      ...toRefs(state),
      initStorageDetails,
      $reset,
    };
  },
);
