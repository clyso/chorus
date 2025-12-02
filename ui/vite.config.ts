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

import { fileURLToPath, URL } from 'node:url';
import { defineConfig } from 'vite';
import vue from '@vitejs/plugin-vue';
import vueDevTools from 'vite-plugin-vue-devtools';
import path from 'path';
import { createSvgIconsPlugin } from 'vite-plugin-svg-icons';

// https://vite.dev/config/
export default defineConfig({
  esbuild: {
    supported: {
      'top-level-await': true,
    },
  },
  build: {
    outDir: 'build',
    emptyOutDir: true,
  },
  plugins: [
    vue({
      script: {
        defineModel: true,
        propsDestructure: true,
      },
    }),
    vueDevTools(),
    createSvgIconsPlugin({
      // Specify the icon folder to be cached
      iconDirs: [
        path.resolve(process.cwd(), 'src/assets/icons'),
        path.resolve(
          process.cwd(),
          'node_modules/@clyso/clyso-ui-kit/dist/assets/icons',
        ),
      ],
      // Specify symbolId format
      symbolId: 'icon-[dir]-[name]',
    }),
  ],
  server: {
    port: 8081,
    proxy: {
      '^/(token|api|spec|prometheus/api.*)': {
        target: 'http://localhost:9671',
        changeOrigin: true,
        secure: true,
      },
    },
  },
  optimizeDeps: {
    include: ['@clyso/clyso-ui-kit'],
  },
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
    },
    preserveSymlinks: true,
  },
  css: {
    devSourcemap: true,
  },
});
