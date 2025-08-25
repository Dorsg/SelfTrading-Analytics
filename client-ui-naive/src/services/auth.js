// src/services/auth.js
import axios from "axios";
import { ref, readonly } from "vue";

axios.defaults.baseURL = "";

/* ──── reactive user object ────────────────────────────────────── */
const _user = ref(null); // <- singleton

function broadcast() {
  window.dispatchEvent(new CustomEvent("auth-user", { detail: _user.value }));
}

function setAuthHeader(token) {
  axios.defaults.headers.common.Authorization = `Bearer ${token}`;
}

function clearAuthHeader() {
  delete axios.defaults.headers.common.Authorization;
}

/* restore user & token on hard refresh */
const savedToken = localStorage.getItem("token");
const savedUser = localStorage.getItem("user");
if (savedToken) setAuthHeader(savedToken);
if (savedUser) _user.value = JSON.parse(savedUser);

/* ──── public helpers ──────────────────────────────────────────── */

// **call only after successful login**
async function fetchMe() {
  const { data } = await axios.get(`/api/auth/me`);
  _user.value = data;
  localStorage.setItem("user", JSON.stringify(data));
  broadcast();
}

export async function signup(payload) {
  return axios.post(`/api/auth/signup`, {
    username: payload.username,
    email: payload.email,
    password: payload.password,
    ib_username: payload.ib_username ?? null,
    ib_password: payload.ib_password ?? null,
  });
}

export async function login(payload) {
  const { data } = await axios.post(`/api/auth/login`, payload);
  localStorage.setItem("token", data.access_token);
  setAuthHeader(data.access_token);
  await fetchMe(); // populate user singleton
  return data.access_token;
}

export function logout() {
  localStorage.removeItem("token");
  localStorage.removeItem("user");
  clearAuthHeader();
  _user.value = null;
  broadcast();
  Object.keys(localStorage).forEach(key => {
    if (key.startsWith("cached::")) {
      localStorage.removeItem(key);
    }
  });
  sessionStorage.clear();
  window.dispatchEvent(new Event("auth-logout"));
}

/* composable-style getter */
export function useCurrentUser() {
  return readonly(_user); // components import and `watch()`
}

/* optional: push user-id on every request */
axios.interceptors.request.use((cfg) => {
  if (_user.value?.id) cfg.headers["x-user-id"] = _user.value.id;
  return cfg;
});

export { setAuthHeader, clearAuthHeader };
