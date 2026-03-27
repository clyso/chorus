/*
 * Copyright © 2026 Clyso GmbH
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

import { ref, type Ref } from 'vue';
import { useNotification, type NotificationConfig } from '@clyso/clyso-ui-kit';

export function useChorusNotification() {
  const {
    createNotification: _createNotification,
    removeNotification: _removeNotification,
  } = useNotification();
  const activeNotificationIds: Ref<string[]> = ref([]);

  function removeNotification(id: string) {
    const index = activeNotificationIds.value.indexOf(id);

    if (index === -1) {
      return;
    }

    activeNotificationIds.value.splice(index, 1);
    setTimeout(() => _removeNotification(id));
  }

  function removeNotifications() {
    activeNotificationIds.value.forEach((id) => {
      setTimeout(() => _removeNotification(id));
    });
    activeNotificationIds.value = [];
  }

  function createNotification(config: NotificationConfig): string {
    const context = { id: '' };
    const extendedConfig = { ...config };

    if (extendedConfig.positiveHandler) {
      const originalHandler = extendedConfig.positiveHandler;

      extendedConfig.positiveHandler = async () => {
        if (context.id) {
          removeNotification(context.id);
        }

        await originalHandler();
      };
    }

    const { id } = _createNotification(extendedConfig).value;

    context.id = id;
    activeNotificationIds.value.push(id);

    return id;
  }

  return {
    activeNotificationIds,
    removeNotification,
    removeNotifications,
    createNotification,
  };
}
