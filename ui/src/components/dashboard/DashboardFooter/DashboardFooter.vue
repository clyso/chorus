<script setup lang="ts">
  import {
    CDashboardFooter,
    CIcon,
    type DashboardFooterLink,
    I18nLocale,
  } from '@clyso/clyso-ui-kit';
  import { computed } from 'vue';
  import { useI18n } from 'vue-i18n';
  import { storeToRefs } from 'pinia';
  import { useI18nStore } from '@/stores/i18nStore';
  import { IconName } from '@/utils/types/icon';

  const year = new Date().getFullYear();

  enum DashboardFooterLinkId {
    ABOUT = 'ABOUT',
    IMPRINT = 'IMPRINT',
    PRIVACY_POLICY = 'PRIVACY_POLICY',
    CONTACT_US = 'CONTACT_US',
  }

  const { t } = useI18n({
    messages: {
      [I18nLocale.EN]: {
        [DashboardFooterLinkId.ABOUT]: 'About Clyso',
        [DashboardFooterLinkId.IMPRINT]: 'Imprint',
        [DashboardFooterLinkId.PRIVACY_POLICY]: 'Privacy Policy',
        [DashboardFooterLinkId.CONTACT_US]: 'Contact Us',
      },
      [I18nLocale.DE]: {
        [DashboardFooterLinkId.ABOUT]: 'Über Clyso',
        [DashboardFooterLinkId.IMPRINT]: 'Impressum',
        [DashboardFooterLinkId.PRIVACY_POLICY]: 'Datenschutzerklärung',
        [DashboardFooterLinkId.CONTACT_US]: 'Kontakt',
      },
    },
  });

  const { locale } = storeToRefs(useI18nStore());

  const footerLinks = computed<DashboardFooterLink[]>(() => {
    return [
      {
        id: DashboardFooterLinkId.ABOUT,
        text: t(DashboardFooterLinkId.ABOUT),
        url:
          locale.value === I18nLocale.DE
            ? 'https://clyso.com/eu/de/about/'
            : 'https://clyso.com/eu/en/about/',
      },
      {
        id: DashboardFooterLinkId.IMPRINT,
        text: t(DashboardFooterLinkId.IMPRINT),
        url:
          locale.value === I18nLocale.DE
            ? 'https://clyso.com/eu/de/impressum/'
            : 'https://clyso.com/eu/en/imprint/',
      },
      {
        id: DashboardFooterLinkId.PRIVACY_POLICY,
        text: t(DashboardFooterLinkId.PRIVACY_POLICY),
        url:
          locale.value === I18nLocale.DE
            ? 'https://clyso.com/eu/de/datenschutz/'
            : 'https://clyso.com/eu/en/privacy-policy/',
      },
      {
        id: DashboardFooterLinkId.CONTACT_US,
        text: t(DashboardFooterLinkId.CONTACT_US),
        url:
          locale.value === I18nLocale.DE
            ? 'https://www.clyso.com/de/kontakt'
            : 'https://www.clyso.com/en/contact',
      },
    ];
  });
</script>

<template>
  <CDashboardFooter
    :is-centered="true"
    :links="footerLinks"
  >
    <template #address>
      Hohenzollernstraße 27, 80801 München, Deutschland
    </template>
    <template #copyright>© Copyright {{ year }} Clyso GmbH</template>
    <template #logo>
      <a
        target="_blank"
        href="https://www.clyso.com/"
        rel="noopener noreferrer"
        class="logo-link"
      >
        <CIcon
          class="logo-link__icon"
          :is-inline="true"
          :name="IconName.BASE_LOGO_CLYSO"
        />
        Clyso
      </a>
    </template>
  </CDashboardFooter>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .logo-link {
    font-size: 0;
    margin-top: - utils.unit(1);

    &__icon {
      width: 100px;
      height: auto;
      color: var(--text-color-3);
      transition: opacity utils.$transition-duration;

      &:hover {
        opacity: 0.6;
      }

      &:active {
        opacity: 0.8;
      }
    }
  }
</style>
