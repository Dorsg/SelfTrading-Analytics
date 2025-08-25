<template>
  <n-config-provider :theme="darkTheme">
    <n-dialog-provider>
      <n-message-provider>
        <n-layout-header class="header">
          <div class="title clickable" @click="goHome" style="display: flex; align-items: center; gap: 10px;">
            <img src="/kraken-svgrepo-com.svg" alt="SelfTrading Icon" style="width: 28px; height: 28px;" />
            <div class="title">SelfTrading</div>
          </div>
          <div v-if="isAuth" class="user-box">
            <!-- Broker connection status -->
            <n-tag
              v-if="statusLoading"
              round
              size="small"
              :bordered="false"
              type="default"
              class="status-label"
            >
              <template #icon>
                <n-spin size="12" />
              </template>
              checking broker...
            </n-tag>

            <n-tag
              v-else
              round
              size="small"
              :bordered="false"
              :type="ibMaintenance ? 'warning' : (ibConnected ? 'success' : 'error')"
              class="status-label"
            >
              <template #icon>
                <n-icon :component="ibMaintenance ? TimeOutline : (ibConnected ? CheckmarkCircleOutline : CloseCircleOutline)" />
              </template>
              {{ ibMaintenance ? 'broker in maintenance' : 'connection to broker' }}
            </n-tag>

            <!-- Market session status -->
            <n-tag
              v-if="marketLoading"
              round
              size="small"
              :bordered="false"
              type="default"
              class="status-label"
            >
              <template #icon>
                <n-spin size="12" />
              </template>
              checking market...
            </n-tag>
            <n-tag
              v-else
              round
              size="small"
              :bordered="false"
              :type="marketStatusColor"
              class="status-label"
            >
              <template #icon>
                <n-icon :component="marketStatusIcon" />
              </template>
              {{ marketStatusLabel }}
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
  CheckmarkCircleOutline,
  CloseCircleOutline,
  TimeOutline
} from "@vicons/ionicons5";

import {
  logout as doLogout,
  useCurrentUser,
  
} from "@/services/auth";
import { fetchIbStatus, fetchMarketStatus  } from '@/services/dataManager';

let inactivityTimer = null;
const INACTIVITY_TIMEOUT_MS = 15 * 60 * 1000; // 15 minutes


const router = useRouter();
const ibConnected = ref(false);
const ibMaintenance = ref(false);
const marketSession = ref();
const statusLoading = ref(true);
const marketLoading = ref(true);
let statusTimer = null;
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

const marketStatusIcon = computed(() => {
  switch (marketSession.value) {
    case "open":
      return CheckmarkCircleOutline; // green
    case "extended-hours":
      return TimeOutline;           // yellow-ish semantic
    case "closed":
    case "close":
    case "":
    case null:
    case undefined:
    default:
      return CloseCircleOutline;    // red / error
  }
});

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

async function refreshStatuses () {
  try {
    const [ib, session] = await Promise.all([
      fetchIbStatus(),          // now returns { connected, maintenance }
      fetchMarketStatus()
    ]);

    ibConnected.value   = !!ib.connected;
    ibMaintenance.value = !!ib.maintenance;

    marketSession.value = session?.toLowerCase?.() || null;
    statusLoading.value = false;
    marketLoading.value = false;
  } catch (e) {
    console.error("Error refreshing statuses:", e);
    ibConnected.value = false;
    ibMaintenance.value = false;
    marketSession.value = null;
    statusLoading.value = false;
    marketLoading.value = false;
  }
}

refreshStatuses().catch((e) => {
  console.error("Initial refreshStatuses failed:", e);
});

onMounted(() => {
  const events = ["mousemove", "keydown", "mousedown", "touchstart"];
  events.forEach((event) => {
    window.addEventListener(event, resetInactivityTimer);
  });

  window.addEventListener("auth-login", refreshStatuses); 

  window.addEventListener("auth-logout", () => {
    if (inactivityTimer) clearTimeout(inactivityTimer);
  });

  resetInactivityTimer(); // start the timer immediately
});

const marketStatusColor = computed(() => {
  // Widget colors per request:
  // a) closed → red
  // b) pre/after (extended-hours) → yellow
  // c) open → green
  // d) no market data → red
  switch (marketSession.value) {
    case "open":
      return "success";        // green
    case "extended-hours":
      return "warning";        // yellow
    case "close":
    case "closed":
      return "error";          // red
    case null:
    case "":
    case undefined:
    default:
      return "error";          // red (no market data)
  }
});

const marketStatusLabel = computed(() => {
  switch (marketSession.value) {
    case "open":
      return "market open";
    case "extended-hours":
      return "pre/after market";
    case "close":
    case "closed":
      return "market closed";
    case null:
    case "":
    case undefined:
    default:
      return "no market data";
  }
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
