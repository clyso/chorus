import { I18nLocale, type I18nMessages } from '@clyso/clyso-ui-kit';

export default <I18nMessages>{
  [I18nLocale.EN]: {
    addReplicationTitle: 'Add Replication',
    step1Title: 'Source Storage',
    step1Description: 'Select storage to replicate from',
    step2Title: 'Destination Storage',
    step2Description: 'Select storage to replicate to',
    step3Title: 'User',
    step3Description: 'Select user',
    step4Title: 'Buckets',
    step4Description: 'Select buckets for replication',
    nextAction: 'Next',
    addReplicationAction: 'Add Replication',
    fromStorageStepTitle: 'Select Source storage for replication:',
    alertTitle: 'Please notice!',
    alertDescription:
      'Currently only Main storage can be selected as Source storage.',
    toStorageStepTitle: 'Select Destination storage for replication:',
    addReplicationError:
      'An error occurred while preparing the Add Replication form. Please try again.',
    userStepTitle: 'Select user:',
    bucketsStepTitle: 'Select buckets for replication:',
    bucketsSelectOption: 'Replicate selected buckets',
    bucketsAllOption: 'Replicate all buckets (including future ones)',
    columnName: 'Bucket',
    columnStatus: 'Status',
    searchBuckets: 'Search buckets',
    showReplicatedBuckets: 'Also show replicated buckets',
    selectedBuckets: '{total} buckets selected',
    statusReplicated: 'Replicated',
    statusAvailable: 'Available for replication',
    initBucketsError:
      'An error occurred while processing the list of buckets. Please try again.',
    noResultsTitle: 'No Buckets found',
    noResultsText:
      'There is no buckets available for replication on {storage} storage for {user} user.',
    filterNoResultsTitle: 'No Buckets found',
    filterNoResultsText: 'There is no buckets matching your search query.',
    filterNoResultsAction: 'Reset search',
    validationErrorTitle: 'Validation Error',
    validationErrorDescription:
      'Please select at least one bucket to create a replication.',
    confirmReplicationTitle: 'Confirm Replication Creation',
    confirmReplicationDescription:
      'You are about to create the following Replication:',
    confirmReplicationPositive: 'Create Replication',
    confirmReplicationNegative: 'Cancel',
    summaryAllBuckets: 'All buckets (including future ones)',
    createReplicationSuccess:
      'Replication(s) were successfully created. You should be able to see them at the top of the list.',
    createReplicationError:
      'An error occurred when creating the replication(s). Please try one more time.',
  },
  [I18nLocale.DE]: {
    addReplicationTitle: 'Add Replication',
    step1Title: 'Source Storage',
    step1Description: 'Select storage to replicate from',
    step2Title: 'Destination Storage',
    step2Description: 'Select storage to replicate to',
    step3Title: 'User',
    step3Description: 'Select user',
    step4Title: 'Buckets',
    step4Description: 'Select buckets for replication',
    nextAction: 'Next',
    addReplicationAction: 'Add Replication',
    fromStorageStepTitle: 'Select Source storage for replication:',
    alertTitle: 'Please notice!',
    alertDescription:
      'Currently only Main storage can be selected as Source storage.',
    toStorageStepTitle: 'Select Destination storage for replication:',
    addReplicationError:
      'An error occurred while preparing the Add Replication form. Please try again.',
    userStepTitle: 'Select user:',
    bucketsStepTitle: 'Select buckets for replication:',
    bucketsSelectOption: 'Replicate selected buckets',
    bucketsAllOption: 'Replicate all buckets (including future ones)',
    columnName: 'Bucket',
    columnStatus: 'Status',
    searchBuckets: 'Search buckets',
    showReplicatedBuckets: 'Also show replicated buckets',
    selectedBuckets: '{total} buckets selected',
    statusReplicated: 'Replicated',
    statusAvailable: 'Available for replication',
    initBucketsError:
      'An error occurred while processing the list of buckets. Please try again.',
    noResultsTitle: 'No Buckets found',
    noResultsText:
      'There is no buckets available for replication on {storage} storage for {user} user.',
    filterNoResultsTitle: 'No Buckets found',
    filterNoResultsText: 'There is no buckets matching your search query.',
    filterNoResultsAction: 'Reset search',
    validationErrorTitle: 'Validation Error',
    validationErrorDescription:
      'Please select at least one bucket to create a replication.',
    summaryAllBuckets: 'All buckets (including future ones)',
    createReplicationSuccess:
      'Replication(s) were successfully created. You should be able to see them at the top of the list.',
    createReplicationError:
      'An error occurred when creating the replication(s). Please try one more time.',
  },
};
