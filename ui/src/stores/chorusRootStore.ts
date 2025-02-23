import { defineStore } from 'pinia';
import { reactive, toRefs } from 'vue';
import type { ChorusProxyCredentials } from '@/utils/types/chorus';
import { ChorusService } from '@/services/ChorusService';

interface ChorusStoragesState {
  proxy: ChorusProxyCredentials | null;
}

function getInitialState(): ChorusStoragesState {
  return {
    proxy: null,
  };
}

export const useChorusRootStore = defineStore('chorusRoot', () => {
  const state = reactive<ChorusStoragesState>(getInitialState());

  async function getProxy() {
    state.proxy = await ChorusService.getProxyCredentials();
  }

  return {
    ...toRefs(state),
    getProxy,
  };
});
