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
