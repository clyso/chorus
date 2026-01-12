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

import { createApp } from 'vue';
import { createPinia } from 'pinia';
import 'virtual:svg-icons-register';
import '@/styles/styles.scss';
import { ColorScheme } from '@clyso/clyso-ui-kit';
import { useI18nStore } from '@/stores/i18nStore';
import { useColorSchemeStore } from '@/stores/colorSchemeStore';
import router from '@/router';
import App from '@/App.vue';
import { i18n } from '@/i18n';

const app = createApp(App);

app.use(createPinia());
app.use(router);
app.use(i18n);

app.mount('#app');

const i18nStore = useI18nStore();
const colorSchemeStore = useColorSchemeStore();

i18nStore.initLocale();
colorSchemeStore.initColorScheme(ColorScheme.DARK);
