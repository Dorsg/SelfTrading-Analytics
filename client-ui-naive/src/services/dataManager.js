// src/services/dataManager.js
import axios from 'axios';

const TTL = 60 * 1000; // 1 minute

function getUserId() {
  try {
    const user = JSON.parse(localStorage.getItem('user'));
    return user?.id || 'default';
  } catch {
    return 'default';
  }
}

function getStorageKey(key) {
  return `cached::${getUserId()}::${key}`;
}

function saveToLocalStorage(key, data) {
  const cacheEntry = {
    timestamp: Date.now(),
    data,
  };
  localStorage.setItem(getStorageKey(key), JSON.stringify(cacheEntry));
}

function getFromLocalStorage(key) {
  const item = localStorage.getItem(getStorageKey(key));
  if (!item) return null;

  try {
    const parsed = JSON.parse(item);
    if (Date.now() - parsed.timestamp < TTL) {
      return parsed.data;
    } else {
      localStorage.removeItem(getStorageKey(key));
    }
  } catch (err) {
    console.warn("Invalid cache entry, clearing:", key);
    localStorage.removeItem(getStorageKey(key));
  }
  return null;
}

async function getCached(key, fetcher, skipCache = false) {
  if (skipCache) {
    const data = await fetcher();
    saveToLocalStorage(key, data);
    return data;
  }

  const cached = getFromLocalStorage(key);
  if (cached !== null) return cached;

  const data = await fetcher();
  saveToLocalStorage(key, data);
  return data;
}

export function invalidateCache(key) {
  localStorage.removeItem(getStorageKey(key));
}

axios.defaults.baseURL = "";

// ─────────── API Calls Using Cached Data ─────────── //

// Analytics-only APIs
export async function fetchProgress() {
  const { data } = await axios.get('/api/analytics/progress', { params: { _t: Date.now() } });
  return data;
}

export async function fetchSimState() {
  const { data } = await axios.get('/api/analytics/simulation/state', { params: { _t: Date.now() } });
  return data;
}

export async function startSimulation() {
  const { data } = await axios.post('/api/analytics/simulation/start');
  return data;
}

export async function stopSimulation() {
  const { data } = await axios.post('/api/analytics/simulation/stop');
  return data;
}

// ─────────── Analytics API Functions ─────────── //

export async function fetchErrors(limit = 50) {
  const { data } = await axios.get(`/api/analytics/errors?limit=${limit}`);
  return data;
}

export async function fetchResults(params = {}) {
  const search = new URLSearchParams(params);
  const { data } = await axios.get(`/api/analytics/results?${search.toString()}`);
  return data;
}
