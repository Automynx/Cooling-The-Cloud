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
});

// API service methods
export const optimizationService = {
  // Run optimization
  runOptimization: async (params) => {
    const response = await api.post('/api/optimize', params);
    return response.data;
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
    const response = await api.get('/api/stats');
    return response.data;
  },

  // Health check
  healthCheck: async () => {
    const response = await api.get('/api/health');
    return response.data;
  },
};

export default api;