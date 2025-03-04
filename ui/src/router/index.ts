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

import { createRouter, createWebHistory } from 'vue-router';
import metaHook from '@/router/guards/meta-hook';
import { routes } from '@/router/routes';

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  scrollBehavior(to, from, savedPosition) {
    if (savedPosition) {
      return savedPosition;
    }

    if (to.params.savePosition === 'TRUE') {
      return {};
    }

    if (to.hash) {
      return {
        selector: to.hash,
        behavior: 'smooth',
      };
    }

    return { x: 0, y: 0 };
  },
  routes,
});

router.afterEach(metaHook);

export default router;
