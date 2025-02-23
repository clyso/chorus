import { defineStore } from 'pinia';
import { computed, reactive, toRefs } from 'vue';
import type { ChorusStorage } from '@/utils/types/chorus';
import { ChorusService } from '@/services/ChorusService';

interface ChorusStoragesState {
  isLoading: boolean;
  hasError: boolean;
  storages: ChorusStorage[];
}

function getInitialState(): ChorusStoragesState {
  return {
    isLoading: false,
    hasError: false,
    storages: [],
  };
}

export const useChorusStoragesStore = defineStore('chorusStorages', () => {
  const state = reactive<ChorusStoragesState>(getInitialState());

  const mainStorage = computed<ChorusStorage | undefined>(() =>
    state.storages.find((storage) => storage.isMain),
  );
  const followerStorages = computed<ChorusStorage[]>(() =>
    state.storages.filter((storage) => !storage.isMain),
  );

  async function initStorages() {
    state.isLoading = true;
    state.hasError = false;

    try {
      const { storages: storagesValue } = await ChorusService.getStorages();

      state.storages = storagesValue;
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
    mainStorage,
    followerStorages,
    initStorages,
    $reset,
  };
});
