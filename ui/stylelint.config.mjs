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

export default {
  plugins: ['stylelint-scss', 'stylelint-prettier'],
  extends: [
    'stylelint-config-recommended-vue/scss',
    'stylelint-config-prettier-scss',
  ],
  rules: {
    'no-descending-specificity': null,
    'at-rule-empty-line-before': null,
    'custom-property-empty-line-before': null,
    'declaration-empty-line-before': null,
    'max-nesting-depth': null,
    'value-keyword-case': null,
    'color-hex-length': ['long'],
    'color-named': 'never',
    'declaration-block-single-line-max-declarations': [0],
    'selector-max-compound-selectors': 4,
    'selector-class-pattern': null,
    'media-feature-name-no-unknown': null,
    'rule-empty-line-before': [
      'always-multi-line',
      {
        except: ['first-nested'],
        ignore: ['after-comment'],
      },
    ],
    'selector-pseudo-element-no-unknown': [
      true,
      {
        ignorePseudoElements: ['v-deep'],
      },
    ],
    'scss/at-function-pattern': null,
    'scss/at-mixin-pattern': null,
    'scss/dollar-variable-pattern': null,
    'scss/percent-placeholder-pattern': null,
    'scss/at-mixin-argumentless-call-parentheses': 'never',
    'scss/no-duplicate-dollar-variables': true,
  },
};
