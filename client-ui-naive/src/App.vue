<template>
  <n-config-provider :theme="darkTheme">
    <n-dialog-provider>
      <n-message-provider>
        <n-layout-header class="header">
          <div class="title clickable" @click="goHome" style="display: flex; align-items: center; gap: 10px;">
            <img src="/kraken-svgrepo-com.svg" alt="SelfTrading Icon" style="width: 28px; height: 28px;" />
            <div class="title">SelfTrading | simulations</div>
          </div>
          <div v-if="isAuth" class="user-box">
            <!-- Analytics Mode Indicator -->
            <n-tag
              round
              size="small"
              :bordered="false"
              type="info"
              class="status-label"
            >
              <template #icon>
                <n-icon :component="AnalyticsOutline" />
              </template>
              Analytics Mode
            </n-tag>

            <span class="uname">{{ capitalizedUsername }}</span>
            <n-tooltip trigger="hover" placement="bottom">
              <template #trigger>
                <n-button circle quaternary size="large" @click="handleLogout">
                  <n-icon :size="27" :component="LogOutOutline" />
                </n-button>
              </template>
              Logout
            </n-tooltip>
          </div>
        </n-layout-header>

        <!-- ─────────── Routed view ─────────── -->
        <RouterView />
      </n-message-provider>
    </n-dialog-provider>
  </n-config-provider>
</template>

<script setup>
import {
  ref,
  computed,
  watch,
  onMounted,
  onBeforeUnmount,
} from "vue";
import { useRouter } from "vue-router";
import { darkTheme } from "naive-ui";
import {
  LogOutOutline,
  AnalyticsOutline
} from "@vicons/ionicons5";

import {
  logout as doLogout,
  useCurrentUser,
  
} from "@/services/auth";


let inactivityTimer = null;
const INACTIVITY_TIMEOUT_MS = 15 * 60 * 1000; // 15 minutes


const router = useRouter();


const user   = useCurrentUser(); // reactive singleton
const isAuth = ref(!!user.value);

watch(user, (v) => (isAuth.value = !!v));

/* Capitalized username */
const capitalizedUsername = computed(() => {
  if (!user.value?.username) return "";
  return user.value.username.charAt(0).toUpperCase() + user.value.username.slice(1);
});

/* ───────── helpers ───────── */
function updateAuth() {
  isAuth.value = !!localStorage.getItem("token");
}



function handleLogout() {
  doLogout();
  router.push({ name: "Login" });
}

function goHome() {
  try {
    localStorage.setItem("activeTab", "account");
  } catch {}
  window.dispatchEvent(new Event("nav-go-account"));
  router.push({ name: "Home" });
}

function resetInactivityTimer() {
  if (inactivityTimer) clearTimeout(inactivityTimer);
  inactivityTimer = setTimeout(() => {
    console.log("Logging out due to inactivity");
    handleLogout();
  }, INACTIVITY_TIMEOUT_MS);
}



onMounted(() => {
  const events = ["mousemove", "keydown", "mousedown", "touchstart"];
  events.forEach((event) => {
    window.addEventListener(event, resetInactivityTimer);
  });

 

  window.addEventListener("auth-logout", () => {
    if (inactivityTimer) clearTimeout(inactivityTimer);
  });

  resetInactivityTimer(); // start the timer immediately
});



onBeforeUnmount(() => {
  if (inactivityTimer) clearTimeout(inactivityTimer);
  const events = ["mousemove", "keydown", "mousedown", "touchstart"];
  events.forEach((event) => {
    window.removeEventListener(event, resetInactivityTimer);
  });

  window.removeEventListener("auth-login", refreshStatuses); 
});
</script>

<style>
html, body, #app { height:100%; margin:0; background:#101014; }

/* header */
.header {
  display:flex;
  align-items:center;
  justify-content:space-between;
  padding:16px;
  border-bottom:1px solid #444;
  background:#18181C;
  color:#fff;
}
.title {
  font-size:20px;
  font-weight:600;
}
.clickable { cursor: pointer; }
.user-box {
  display:flex;
  align-items:center;
  gap:14px;
}
.uname {
  font-size:16px;  /* bigger font */
  font-weight:600;
}
.status-label {
  font-weight: 600;
}
</style>
