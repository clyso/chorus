import pluginVue from 'eslint-plugin-vue';
import vueTsEslintConfig from '@vue/eslint-config-typescript';
import pluginVitest from '@vitest/eslint-plugin';
import oxlint from 'eslint-plugin-oxlint';
import skipFormatting from '@vue/eslint-config-prettier/skip-formatting';
import importPlugin from 'eslint-plugin-import';

export default [
  {
    name: 'app/files-to-lint',
    files: ['**/*.{ts,mts,tsx,vue}'],
  },

  {
    name: 'app/files-to-ignore',
    ignores: ['**/dist/**', '**/build/**', '**/dist-ssr/**', '**/coverage/**'],
  },

  // Essential Vue and TypeScript configurations
  ...pluginVue.configs['flat/essential'],
  ...vueTsEslintConfig(),

  // Vitest config for test files
  {
    ...pluginVitest.configs.recommended,
    files: ['src/**/__tests__/*'],
  },
  oxlint.configs['flat/recommended'],
  skipFormatting,
  {
    plugins: {
      import: importPlugin,
    },
    rules: {
      'vue/html-button-has-type': [
        'error',
        {
          button: true,
          submit: true,
          reset: true,
        },
      ],
      'vue/max-attributes-per-line': [
        'error',
        {
          singleline: {
            max: 1,
          },
          multiline: {
            max: 1,
          },
        },
      ],
      'vue/component-tags-order': [
        'error',
        { order: ['script', 'template', 'style'] },
      ],
      'vue/padding-line-between-blocks': ['error', 'always'],

      '@typescript-eslint/no-shadow': ['error'],
      '@typescript-eslint/no-unused-vars': [
        'error',
        { ignoreRestSiblings: true },
      ],
      '@typescript-eslint/no-use-before-define': [
        'error',
        { functions: false },
      ],
      '@typescript-eslint/no-explicit-any': ['error'],

      'padding-line-between-statements': [
        'error',
        {
          blankLine: 'always',
          prev: '*',
          next: ['return', 'if', 'try', 'for', 'function'],
        },
        { blankLine: 'always', prev: ['if', 'case', 'default'], next: '*' },
        { blankLine: 'always', prev: ['const', 'let', 'var'], next: '*' },
        {
          blankLine: 'any',
          prev: ['const', 'let', 'var'],
          next: ['const', 'let', 'var'],
        },
      ],

      'import/order': [
        'error',
        {
          groups: [
            ['builtin', 'external'],
            ['internal'],
            ['sibling', 'parent'],
            ['index'],
          ],
          'newlines-between': 'never',
        },
      ],
      'no-console': ['error'],
    },
  },
];
