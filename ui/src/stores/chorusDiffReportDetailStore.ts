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

import { defineStore } from 'pinia';
import { computed, reactive, toRefs } from 'vue';
import { useRoute } from 'vue-router';
import type { DiffReport, DiffReportLocation } from '@/utils/types/chorus';
import { ChorusService } from '@/services/ChorusService';
import { DiffReportsHelper } from '@/utils/helpers/DiffReportsHelper';
import { ErrorHelper } from '@/utils/helpers/ErrorHelper';

interface ChorusDiffReportDetailState {
  isLoading: boolean;
  isRestartLoading: boolean;
  hasError: boolean;
  isNotFound: boolean;
  locations: DiffReportLocation[];
  report: DiffReport | null;
  pollingRequest: Promise<unknown> | null;
  pollingTimeout: number | null;
}

const POLL_INTERVAL_MS = 5000;

function getInitialState(): ChorusDiffReportDetailState {
  return {
    isLoading: false,
    isRestartLoading: false,
    hasError: false,
    isNotFound: false,
    locations: [],
    report: null,
    pollingRequest: null,
    pollingTimeout: null,
  };
}

export const useChorusDiffReportDetailStore = defineStore(
  'chorusDiffReportDetail',
  () => {
    const state = reactive<ChorusDiffReportDetailState>(getInitialState());
    const route = useRoute();

    state.locations = DiffReportsHelper.parseQueryParamsToLocations(
      route.query.from,
      route.query.to,
    );

    const hasFixActivity = computed(() => {
      if (!state.report) return false;

      return (
        !state.report.consistent &&
        (Number(state.report.fixQueued) > 0 ||
          Number(state.report.fixCompleted) > 0 ||
          state.report.fixReady)
      );
    });

    async function getDiffReport() {
      if (!state.locations) return;

      const res = await ChorusService.getDiffReport({
        locations: state.locations,
      });

      state.report = res.check;
    }

    async function startPolling() {
      try {
        stopPolling();

        state.pollingRequest = getDiffReport();

        await state.pollingRequest;
      } catch (e) {
        if (ErrorHelper.getStatusCode(e) === 404) {
          state.isNotFound = true;

          return;
        }

        throw e;
      } finally {
        state.pollingRequest = null;
      }

      state.pollingTimeout = window.setTimeout(startPolling, POLL_INTERVAL_MS);
    }

    function stopPolling() {
      if (state.pollingTimeout) {
        clearTimeout(state.pollingTimeout);
        state.pollingTimeout = null;
      }
    }

    async function initDiffReportDetailsPage() {
      if (state.locations.length !== 2) {
        return;
      }

      state.isLoading = true;

      try {
        await startPolling();

        state.hasError = false;
      } catch {
        state.hasError = true;
        stopPolling();
      } finally {
        state.isLoading = false;
      }
    }

    async function restartDiffReport() {
      state.isRestartLoading = true;

      stopPolling();

      try {
        await ChorusService.restartDiffReport({
          locations: state.locations,
        });
      } finally {
        state.isRestartLoading = false;
        await startPolling();
      }
    }

    function $reset() {
      try {
        stopPolling();
      } finally {
        Object.assign(state, getInitialState());
      }
    }

    return {
      ...toRefs(state),
      initDiffReportDetailsPage,
      restartDiffReport,
      hasFixActivity,
      $reset,
    };
  },
);
