<template>
  <n-config-provider :theme="darkTheme">
    <n-dialog-provider>
      <n-message-provider>
        <n-layout-header class="header">
          <div class="title clickable" @click="goHome" style="display: flex; align-items: center; gap: 10px;">
            <img src="/kraken-svgrepo-com.svg" alt="SelfTrading Icon" style="width: 28px; height: 28px;" />
            <div class="title">SelfTrading | simulations</div>
          </div>
          <div class="user-box">
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

            <span class="uname">Analytics User</span>
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
  onMounted,
  onBeforeUnmount,
} from "vue";
import { useRouter } from "vue-router";
import { darkTheme } from "naive-ui";
import {
  AnalyticsOutline
} from "@vicons/ionicons5";

// Analytics mode - no authentication required
const router = useRouter();
const user = ref({ username: "analytics" });
const isAuth = ref(true); // Always authenticated in analytics mode

/* ───────── helpers ───────── */
function goHome() {
  try {
    localStorage.setItem("activeTab", "account");
  } catch {}
  window.dispatchEvent(new Event("nav-go-account"));
  router.push({ name: "Home" });
}

// Analytics mode - no inactivity logout needed
function resetInactivityTimer() {
  // No-op in analytics mode
}

onMounted(() => {
  // Analytics mode - no authentication events needed
});

onBeforeUnmount(() => {
  // Analytics mode - no cleanup needed
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
