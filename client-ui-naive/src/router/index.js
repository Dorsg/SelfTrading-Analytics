// client-ui-naive/src/router/index.js
import { createRouter, createWebHistory } from "vue-router";
import LoginPage   from "@/components/LoginPage.vue";
import Dashboard   from "@/pages/Dashboard.vue";
import { logout }  from "@/services/auth";

const routes = [
  { path: "/login", component: LoginPage, name: "Login" },
  { path: "/", component: Dashboard, name: "Home", meta: { requiresAuth: true } },
];

const router = createRouter({
  history: createWebHistory(),
  routes,
});

router.beforeEach((to, _from, next) => {
  const token = localStorage.getItem("token");
  if (to.meta.requiresAuth && !token) {
    next({ name: "Login", query: { next: to.fullPath } });
  } else {
    next();
  }
});

// global 401 → auto-logout
import axios from "axios";
axios.interceptors.response.use(
  r => r,
  err => {
    if (err.response?.status === 401) {
      logout();
      router.push({ name: "Login" });
      window.dispatchEvent(new Event("auth-logout"));
    }
    return Promise.reject(err);
  },
);

export default router;
