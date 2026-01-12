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

import { computed, ref } from 'vue';
import { defineStore } from 'pinia';
import { ColorScheme } from '@clyso/clyso-ui-kit';
import LocalStorageHelper from '@/utils/helpers/LocalStorageHelper';
import { LocalStorageItem } from '@/utils/types/localStorage';

const SYSTEM_COLOR_SCHEME: ColorScheme = window.matchMedia(
  '(prefers-color-scheme: dark)',
).matches
  ? ColorScheme.DARK
  : ColorScheme.LIGHT;

export const useColorSchemeStore = defineStore('colorScheme', () => {
  const colorSchemeRef = ref(SYSTEM_COLOR_SCHEME);

  const colorScheme = computed(() => colorSchemeRef.value);

  const isDark = computed(() => colorSchemeRef.value === ColorScheme.DARK);

  function initColorScheme(defaultColorScheme = SYSTEM_COLOR_SCHEME) {
    const savedColorScheme = LocalStorageHelper.get<ColorScheme>(
      LocalStorageItem.COLOR_SCHEME,
    );

    colorSchemeRef.value =
      savedColorScheme && ColorScheme[savedColorScheme]
        ? savedColorScheme
        : defaultColorScheme;
    setColorSchemeClass();
  }

  function setColorScheme(newColorScheme: ColorScheme) {
    if (newColorScheme === colorSchemeRef.value) {
      return;
    }

    colorSchemeRef.value = newColorScheme;
    LocalStorageHelper.set(LocalStorageItem.COLOR_SCHEME, newColorScheme);
    setColorSchemeClass();
  }

  function setIsDark(value: boolean) {
    setColorScheme(value ? ColorScheme.DARK : ColorScheme.LIGHT);
  }

  function setColorSchemeClass() {
    if (colorSchemeRef.value === ColorScheme.DARK) {
      document.body.classList.add('color-scheme-dark');

      return;
    }

    document.body.classList.remove('color-scheme-dark');
  }

  return {
    colorScheme,
    isDark,
    setColorScheme,
    setIsDark,
    initColorScheme,
  };
});
