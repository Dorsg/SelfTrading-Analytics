// client-ui-naive/src/router/index.js
import { createRouter, createWebHistory } from "vue-router";
import LoginPage   from "@/components/LoginPage.vue";
import Dashboard   from "@/pages/Dashboard.vue";
import { logout }  from "@/services/auth";

const routes = [
  { path: "/login", component: LoginPage, name: "Login" },
  { path: "/", component: Dashboard, name: "Home" }, // No auth required for analytics
];

const router = createRouter({
  history: createWebHistory(),
  routes,
});

// Analytics mode - no authentication required
router.beforeEach((to, _from, next) => {
  // Skip authentication checks for analytics-only app
  next();
});

// Analytics mode - no 401 handling needed since no auth required
import axios from "axios";

export default router;
