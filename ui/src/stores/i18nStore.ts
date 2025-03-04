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

import { computed } from 'vue';
import { defineStore } from 'pinia';
import { I18N_DEFAULT_LOCALE, I18nLocale } from '@clyso/clyso-ui-kit';
import { i18n } from '@/i18n';
import LocalStorageHelper from '@/utils/helpers/LocalStorageHelper';
import { LocalStorageItem } from '@/utils/types/localStorage';

export const useI18nStore = defineStore('i18n', () => {
  const locale = computed<I18nLocale>(
    () => i18n.global.locale.value as I18nLocale,
  );
  const localeList = computed(() => Object.values(I18nLocale));

  function updateLocaleValue(newLocale: I18nLocale) {
    i18n.global.locale.value = newLocale;
  }

  function setLocale(newLocale: I18nLocale) {
    updateLocaleValue(newLocale);
    LocalStorageHelper.set(LocalStorageItem.I18N_LOCALE, newLocale);
  }

  function initLocale() {
    const restoredLocale: I18nLocale =
      LocalStorageHelper.get<I18nLocale>(LocalStorageItem.I18N_LOCALE) ||
      (navigator.language.slice(0, 2) as I18nLocale);

    updateLocaleValue(
      localeList.value.includes(restoredLocale)
        ? restoredLocale
        : I18N_DEFAULT_LOCALE,
    );
  }

  return {
    locale,
    localeList,
    setLocale,
    initLocale,
  };
});
