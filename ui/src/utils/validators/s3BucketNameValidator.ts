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

// See https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html#general-purpose-bucket-names
// Field must be filled out
export function isRequired(
  isForAllBuckets: boolean,
  val: string | null,
): boolean {
  if (isForAllBuckets) return true;

  if (!val) return false;

  return val.trim() !== '';
}

// Length must be between 3 and 63 characters
export function isValidLength(
  isForAllBuckets: boolean,
  val: string | null,
): boolean {
  if (isForAllBuckets || !val) return true;

  return val.length >= 3 && val.length <= 63;
}

// Can only contain lowercase letters, numbers, dots, and hyphens
export function hasValidChars(
  isForAllBuckets: boolean,
  val: string | null,
): boolean {
  if (isForAllBuckets || !val) return true;

  return /^[a-z0-9.-]+$/.test(val);
}

// Must begin and end with a letter or number
export function hasValidStartEnd(
  isForAllBuckets: boolean,
  val: string | null,
): boolean {
  if (isForAllBuckets || !val) return true;

  return /^[a-z0-9]/.test(val) && /[a-z0-9]$/.test(val);
}

// Must not contain two adjacent periods
export function hasNoAdjacentPeriods(
  isForAllBuckets: boolean,
  val: string | null,
): boolean {
  if (isForAllBuckets || !val) return true;

  return !/\.\./.test(val);
}

// Must not be formatted as an IP address (e.g., 192.168.5.4)
export function isNotIpAddress(
  isForAllBuckets: boolean,
  val: string | null,
): boolean {
  if (isForAllBuckets || !val) return true;

  return !/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(val);
}

// AWS specific prefix/suffix restrictions
export function hasValidPrefixSuffix(
  isForAllBuckets: boolean,
  val: string | null,
): boolean {
  if (isForAllBuckets || !val) return true;

  if (
    val.startsWith('xn--') ||
    val.startsWith('sthree-') ||
    val.startsWith('amzn-s3-demo-')
  )
    return false;

  if (
    val.endsWith('-s3alias') ||
    val.endsWith('--ol-s3') ||
    val.endsWith('.mrap') ||
    val.endsWith('--x-s3') ||
    val.endsWith('--table-s3')
  )
    return false;

  return true;
}
