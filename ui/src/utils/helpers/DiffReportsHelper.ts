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

import { DiffReportStatusFilter, type DiffReport } from '@/utils/types/chorus';

export abstract class DiffReportsHelper {
  static getDiffReportId(locations: DiffReport['locations']): string {
    return locations.map((l) => `${l.storage}:${l.bucket}`).join('|');
  }

  static getBucketPair(report: DiffReport): string {
    if (report.locations.length !== 2) {
      return '';
    }

    return `${report.locations[0]?.bucket} → ${report.locations[1]?.bucket}`;
  }

  static getDirectionPair(report: DiffReport): string {
    if (report.locations.length !== 2) {
      return '';
    }

    return `${report.locations[0]?.storage} → ${report.locations[1]?.storage}`;
  }

  static isDiffReportStatusMatched(
    diffReport: DiffReport,
    status: DiffReportStatusFilter,
  ): boolean {
    if (status === DiffReportStatusFilter.CHECKING) {
      return !diffReport.ready;
    }

    if (status === DiffReportStatusFilter.CONSISTENT) {
      return diffReport.ready && diffReport.consistent;
    }

    if (status === DiffReportStatusFilter.INCONSISTENT) {
      return diffReport.ready && !diffReport.consistent;
    }

    return false;
  }

  static getStatusSortOrder(report: DiffReport): number {
    if (!report.ready) return 0; // checking

    if (report.consistent) return 1; // consistent

    return 2; // inconsistent
  }
}
