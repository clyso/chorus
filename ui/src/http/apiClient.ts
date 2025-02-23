import axios, { AxiosError } from 'axios';
import { LogHelper } from '@/utils/helpers/LogHelper';
import { useAuthStore } from '@/stores/authStore';
import { HAS_AUTH } from '@/utils/constants/env';

const axiosInstance = axios.create();

axiosInstance.interceptors.response.use(
  (response) => response,
  (error: AxiosError) => {
    if (
      error.response?.status === 401 &&
      useAuthStore().isAuthenticated &&
      HAS_AUTH
    ) {
      const { logout } = useAuthStore();

      logout({
        isChannelMessageForced: true,
        isRedirectRouteSaved: true,
      });

      LogHelper.log('AUTH: 401 logging out');
    }

    return Promise.reject(error);
  },
);

export default axiosInstance;
