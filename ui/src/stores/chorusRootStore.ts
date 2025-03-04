/*
 * Copyright © 2025 Clyso GmbH
 *
 *  Licensed under the GNU Affero General Public License, Version 3.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  https://www.gnu.org/licenses/agpl-3.0.html
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

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
