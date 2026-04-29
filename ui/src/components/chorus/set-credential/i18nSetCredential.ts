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

import { I18nLocale, type I18nMessages } from '@clyso/clyso-ui-kit';

export default <I18nMessages>{
  [I18nLocale.EN]: {
    breadcrumbStorages: 'Storages',
    breadcrumbAddCredential: 'Add Credential',
    breadcrumbEditCredential: 'Edit Credential: {alias}',
    setCredentialTitleAdd: 'Add Credential',
    setCredentialTitleEdit: 'Edit Credential: {user}',
    setCredentialHeader:
      'Provide the credential details for storage "{storage}".',
    fieldUserLabel: 'User',
    fieldAccessKeyLabel: 'Access Key',
    fieldSecretKeyLabel: 'Secret Key',
    fieldUsernameLabel: 'Username',
    fieldPasswordLabel: 'Password',
    fieldDomainNameLabel: 'Domain Name',
    fieldTenantNameLabel: 'Tenant Name',
    validationUserRequired: 'Please enter a user alias.',
    validationAccessKeyRequired: 'Please enter an access key.',
    validationSecretKeyRequired: 'Please enter a secret key.',
    validationUsernameRequired: 'Please enter a username.',
    validationPasswordRequired: 'Please enter a password.',
    validationDomainNameRequired: 'Please enter a domain name.',
    validationTenantNameRequired: 'Please enter a tenant name.',
    actionSetCredential: 'Set Credential',
    confirmTitle: 'Confirm Credential Update',
    confirmDescription:
      "You're about to set the credentials for the user. Do you want to proceed?",
    confirmPositive: 'Set Credentials',
    confirmNegative: 'Cancel',
    successTitle: 'Credential saved!',
    successContent: 'The credential has been saved successfully.',
    errorTitle: 'Credential not saved!',
    errorContent: 'An error occurred while saving the credential:',
    errorAction: 'Retry',
    submitErrorUnknown:
      'An unknown error occurred while saving the credential.',
    submitErrorUnknownStorage:
      'Cannot save credentials: no storage is selected.',
  },
  [I18nLocale.DE]: {
    breadcrumbStorages: 'Speicher',
    breadcrumbAddCredential: 'Zugangsdaten hinzufügen',
    breadcrumbEditCredential: 'Zugangsdaten bearbeiten: {alias}',
    setCredentialTitleAdd: 'Zugangsdaten hinzufügen',
    setCredentialTitleEdit: 'Zugangsdaten bearbeiten: {user}',
    setCredentialHeader:
      'Geben Sie die Zugangsdaten für den Speicher "{storage}" ein.',
    fieldUserLabel: 'Benutzer',
    fieldAccessKeyLabel: 'Zugriffsschlüssel',
    fieldSecretKeyLabel: 'Geheimer Schlüssel',
    fieldUsernameLabel: 'Benutzername',
    fieldPasswordLabel: 'Passwort',
    fieldDomainNameLabel: 'Domainname',
    fieldTenantNameLabel: 'Mandantenname',
    validationUserRequired: 'Bitte geben Sie einen Benutzeralias ein.',
    validationAccessKeyRequired: 'Bitte geben Sie einen Zugriffsschlüssel ein.',
    validationSecretKeyRequired:
      'Bitte geben Sie einen geheimen Schlüssel ein.',
    validationUsernameRequired: 'Bitte geben Sie einen Benutzernamen ein.',
    validationPasswordRequired: 'Bitte geben Sie ein Passwort ein.',
    validationDomainNameRequired: 'Bitte geben Sie einen Domainnamen ein.',
    validationTenantNameRequired: 'Bitte geben Sie einen Mandantennamen ein.',
    actionSetCredential: 'Zugangsdaten setzen',
    confirmTitle: 'Zugangsdaten-Aktualisierung bestätigen',
    confirmDescription:
      'Sie sind dabei, die Zugangsdaten für den Benutzer zu setzen. Möchten Sie fortfahren?',
    confirmPositive: 'Zugangsdaten setzen',
    confirmNegative: 'Abbrechen',
    successTitle: 'Zugangsdaten gespeichert!',
    successContent: 'Die Zugangsdaten wurden erfolgreich gespeichert.',
    errorTitle: 'Zugangsdaten nicht gespeichert!',
    errorContent: 'Beim Speichern der Zugangsdaten ist ein Fehler aufgetreten:',
    errorAction: 'Erneut versuchen',
    submitErrorUnknown:
      'Beim Speichern der Zugangsdaten ist ein unbekannter Fehler aufgetreten.',
    submitErrorUnknownStorage:
      'Zugangsdaten können nicht gespeichert werden: Es ist kein Speicher ausgewählt.',
  },
};
