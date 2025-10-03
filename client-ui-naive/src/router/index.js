// client-ui-naive/src/router/index.js
import { createRouter, createWebHistory } from "vue-router";
import Dashboard   from "@/pages/Dashboard.vue";

const routes = [
  { path: "/", component: Dashboard, name: "Home" }, // No auth required for analytics
];

const router = createRouter({
  history: createWebHistory(),
  routes,
});

// Analytics mode - no authentication required
router.beforeEach((_to, _from, next) => {
  next();
});

export default router;
