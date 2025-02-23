<script setup lang="ts">
  import { useI18n } from 'vue-i18n';
  import { CIcon } from '@clyso/clyso-ui-kit';
  import i18nLoginPage from '@/components/auth/LoginPage/i18nLoginPage';
  import { IconName } from '@/utils/types/icon';

  const { t } = useI18n({
    messages: i18nLoginPage,
  });
</script>

<template>
  <div class="auth-page-content">
    <div class="container">
      <div class="auth-page-content__inner">
        <div class="content-card">
          <div class="content-card__left">
            <div class="info">
              <h1 class="info__title">
                <span>Chorus</span>
              </h1>
            </div>
          </div>

          <div class="content-card__right">
            <div class="main-content">
              <div class="main-content__header">
                <div class="main-content__header-logo">
                  <div class="main-content__header-info info">
                    <h1 class="info__title">
                      <span>Chorus</span>
                    </h1>
                  </div>

                  <div class="main-content__logo">
                    <CIcon
                      class="main-content__logo-icon"
                      :name="IconName.CUSTOM_CHORUS_LOGO"
                    />
                  </div>
                </div>

                <div class="main-content__greeting">
                  {{ t('greeting') }}
                </div>
              </div>

              <div class="main-content__main">
                <slot></slot>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<style lang="scss" scoped>
  @use '@/styles/utils' as utils;

  .auth-page-content {
    display: grid;
    align-items: center;
    padding-top: utils.unit(10);
    padding-bottom: utils.unit(40);

    @include utils.tablet-only {
      justify-content: center;
    }

    @include utils.touch {
      padding-top: utils.unit(6);
    }
  }

  .content-card {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    position: relative;
    max-width: 1100px;
    margin: 0 auto;
    border-radius: 4px;
    overflow: hidden;
    border: 1px solid var(--primary-color);

    @include utils.tablet-only {
      display: block;
      min-width: 500px;
    }

    @include utils.mobile {
      display: block;
      width: 100%;
    }

    &::before {
      content: '';
      @include utils.absolute-fit;
      background-color: var(--body-color);
      opacity: 0.7;
      z-index: -1;

      @include utils.touch {
        background-image: none;
      }
    }

    &__left {
      padding: utils.unit(10);
      padding-right: utils.unit(6);
      position: relative;
      display: grid;
      align-items: center;

      @include utils.touch {
        display: none;
      }

      &::after {
        content: '';
        height: 80%;
        border-right: 1px solid var(--text-color-base);
        @include utils.absolute-y-center;
        right: 0;
      }
    }

    &__right {
      padding: utils.unit(10);
      padding-left: utils.unit(6);

      @include utils.touch {
        padding: utils.unit(6) utils.unit(4);
      }
    }
  }

  .info {
    text-align: left;

    &__title {
      font-family: utils.$font-highlight;
      font-size: 100px;
      font-weight: utils.$font-weight-semibold;
      line-height: 0.8;
      margin-bottom: utils.unit(1);
      letter-spacing: -0.08em;

      @include utils.touch {
        font-size: 20px;
        margin-bottom: utils.unit(1);
      }

      span {
        display: block;
      }
    }

    &__text {
      font-size: 18px;
      color: var(--primary-color);
      padding-left: utils.unit(1);

      @include utils.touch {
        display: none;
      }
    }
  }

  .main-content {
    display: grid;

    &__header {
      display: grid;
      justify-items: center;
      margin-bottom: utils.unit(10);
    }

    &__header-info {
      display: none;

      @include utils.touch {
        display: block;
      }
    }

    &__header-logo {
      display: flex;
      align-items: center;
      justify-content: center;
      margin-bottom: utils.unit(1);
      width: 100%;

      @include utils.touch {
        justify-content: space-between;
        margin-bottom: utils.unit(4);
      }
    }

    &__logo-icon {
      width: 70px;
      height: 70px;
      color: var(--primary-color);

      @include utils.touch {
        width: 40px;
        height: 40px;
      }
    }

    &__greeting {
      font-size: 20px;
      font-weight: utils.$font-weight-medium;
      text-align: center;
    }

    &__main {
      display: grid;
      width: 100%;
      justify-items: center;
    }
  }
</style>
