import axios from 'axios';

// API configuration
const API_BASE_URL = process.env.NODE_ENV === 'production'
  ? ''  // Same domain when both frontend and API are on Vercel
  : 'http://localhost:5000';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
  timeout: 30000, // 30 second timeout
});

// Fallback to static demo data
const loadDemoData = async () => {
  try {
    const response = await fetch('/demo-data.json');
    return await response.json();
  } catch (error) {
    console.error('Failed to load demo data:', error);
    return null;
  }
};

// Wrapper for API calls with fallback
const apiCallWithFallback = async (apiCall, fallbackKey) => {
  try {
    return await apiCall();
  } catch (error) {
    console.warn(`API call failed: ${error.message}. Attempting fallback to demo data...`);

    // Try to load static demo data as fallback
    const demoData = await loadDemoData();
    if (demoData && fallbackKey && demoData[fallbackKey]) {
      console.info(`Using static demo data for ${fallbackKey}`);
      return demoData[fallbackKey];
    }

    // If no fallback available, throw the original error
    throw error;
  }
};

// API service methods
export const optimizationService = {
  // Run optimization
  runOptimization: async (params) => {
    return apiCallWithFallback(
      async () => {
        const response = await api.post('/api/optimize', params);
        return response.data;
      },
      'optimize'
    );
  },

  // Get optimization history
  getHistory: async (limit = 10) => {
    const response = await api.get('/api/history', { params: { limit } });
    return response.data;
  },

  // Get period summary
  getPeriodSummary: async (days = 30) => {
    const response = await api.get('/api/period-summary', { params: { days } });
    return response.data;
  },

  // Get monthly breakdown
  getMonthlyBreakdown: async (months = 6) => {
    const response = await api.get('/api/monthly-breakdown', { params: { months } });
    return response.data;
  },

  // Get daily trends
  getDailyTrends: async (days = 30) => {
    const response = await api.get('/api/daily-trends', { params: { days } });
    return response.data;
  },

  // Get real-time data
  getRealTimeData: async (date) => {
    const response = await api.get('/api/real-time-data', { params: { date } });
    return response.data;
  },

  // Get system stats
  getStats: async () => {
    return apiCallWithFallback(
      async () => {
        const response = await api.get('/api/stats');
        return response.data;
      },
      'stats'
    );
  },

  // Health check
  healthCheck: async () => {
    const response = await api.get('/api/health');
    return response.data;
  },
};

export default api;