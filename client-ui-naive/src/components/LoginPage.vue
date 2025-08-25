<template>
  <n-config-provider :theme="darkTheme">
    <n-message-provider>
      <n-layout class="login-layout">
        <n-card
          class="login-card"
          :title="isLogin ? 'Login to SelfTrading' : 'Create an Account'"
          :style="{ width: isLogin ? '360px' : '720px' }"
        >
          <n-form
            ref="formRef"
            :model="form"
            :rules="rules"
            label-placement="top"
            label-width="auto"
            require-mark-placement="right-hanging"
            @submit.prevent="isLogin ? handleLogin() : handleSignup()"
          >
            <!-- Username -->
            <n-form-item label="Username" path="username">
              <n-input
                v-model:value="form.username"
                placeholder="Enter your username"
                :input-props="{ autocomplete: 'username' }"
              />
            </n-form-item>

            <!-- Password -->
            <n-form-item label="Password" path="password">
              <n-input
                type="password"
                show-password-on="click"
                v-model:value="form.password"
                placeholder="Enter your password"
                :input-props="{
                  autocomplete: isLogin ? 'current-password' : 'new-password',
                }"
              />
            </n-form-item>

            <!-- Sign-Up only -->
            <template v-if="!isLogin">
              <n-grid :cols="2" :x-gap="16">
                <n-form-item-gi label="Confirm Password" path="confirmPassword">
                  <n-input
                    type="password"
                    show-password-on="click"
                    v-model:value="form.confirmPassword"
                    placeholder="Confirm your password"
                    :input-props="{ autocomplete: 'new-password' }"
                  />
                </n-form-item-gi>

                <n-form-item-gi label="Email" path="email">
                  <n-input
                    type="email"
                    v-model:value="form.email"
                    placeholder="Enter your email"
                    :input-props="{ autocomplete: 'email' }"
                  />
                </n-form-item-gi>

                <n-form-item-gi label="IB Username" path="ibUser">
                  <n-input
                    v-model:value="form.ibUser"
                    placeholder="Enter your IB username"
                  />
                </n-form-item-gi>

                <n-form-item-gi label="IB Password" path="ibPassword">
                  <n-input
                    type="password"
                    show-password-on="click"
                    v-model:value="form.ibPassword"
                    placeholder="Enter your IB password"
                  />
                </n-form-item-gi>
              </n-grid>
            </template>

            <!-- Submit -->
            <n-form-item>
              <n-button
                type="primary"
                block
                :loading="loading"
                @click="isLogin ? handleLogin() : handleSignup()"
              >
                {{ isLogin ? "Login" : "Sign Up" }}
              </n-button>
            </n-form-item>
          </n-form>

          <!-- Switch mode -->
          <div class="switch-auth-mode">
            <n-button text @click="isLogin = !isLogin">
              {{
                isLogin
                  ? "Don't have an account? Sign Up"
                  : "Already have an account? Login"
              }}
            </n-button>
          </div>
        </n-card>
      </n-layout>
    </n-message-provider>
  </n-config-provider>
</template>

<script setup>
import { ref } from "vue";
import { darkTheme, useMessage } from "naive-ui";
import { login as apiLogin, signup as apiSignup } from "@/services/auth";
import { useRouter, useRoute } from "vue-router";

const emit = defineEmits(["login-success"]);

const formRef = ref(null);
const loading = ref(false);
const isLogin = ref(true);
const message = useMessage();
const router  = useRouter();
const route   = useRoute();

const form = ref({
  username: "",
  password: "",
  confirmPassword: "",
  email: "",
  ibUser: "",
  ibPassword: "",
});

/* ------------- helpers ---------------- */
function fastapiErrorToString(err) {
  const d = err.response?.data?.detail;
  if (Array.isArray(d)) return d.map(o => o.msg).join("; ");
  if (typeof d === "string") return d;
  return err.message || "Unknown error";
}

/* ------------- rules (unchanged) --------------- */
const rules = {
  username: { required: true, message: "Username is required", trigger: "blur" },
  password: {
    required: true,
    trigger: "blur",
    validator: (_, v) =>
      v && v.length >= 6 ? true : new Error("Password must be at least 6 characters"),
  },
  confirmPassword: {
    required: () => !isLogin.value,
    trigger: "blur",
    validator: (_, v) =>
      !isLogin.value && v !== form.value.password
        ? new Error("Passwords do not match")
        : true,
  },
  email: {
    required: () => !isLogin.value,
    trigger: "blur",
    validator: (_, v) =>
      /^\S+@\S+\.\S+$/.test(v) ? true : new Error("Invalid email"),
  },
  ibUser:      { required: () => !isLogin.value, message: "IB username is required" },
  ibPassword:  { required: () => !isLogin.value, message: "IB password is required" },
};

/* ------------- actions ---------------- */
function handleLogin() {
  formRef.value?.validate(async (err) => {
    if (err) return message.error("Please fix form errors");
    loading.value = true;
    try {
      await apiLogin({ username: form.value.username, password: form.value.password });

      emit("login-success");

      message.success("Login successful");
      await router.push(route.query.next ?? { name: "Home" });
      window.dispatchEvent(new Event("auth-login"));
    } catch (e) {
      message.error(fastapiErrorToString(e));
    } finally {
      loading.value = false;
    }
  });
}

function handleSignup() {
  formRef.value?.validate(async (err) => {
    if (err) return message.error("Please fix form errors");
    loading.value = true;
    try {
      await apiSignup({
        username: form.value.username,
        email:    form.value.email,
        password: form.value.password,
        ib_username: form.value.ibUser || null,
        ib_password: form.value.ibPassword || null,
      });
      message.success("Sign-up successful, you can now log in");
      isLogin.value = true;
      form.value.password = form.value.confirmPassword = "";
    } catch (e) {
      message.error(fastapiErrorToString(e));
    } finally {
      loading.value = false;
    }
  });
}
</script>


<style scoped>
.login-layout {
  display: flex;
  justify-content: center;
  align-items: center;
  min-height: calc(100vh - 130px);
}
.login-card {
  padding: 24px;
  background: #1a1a1a;
  border: 1px solid #444;
  box-shadow: 0 2px 12px rgba(0, 0, 0, 0.4);
  color: white;
}
.switch-auth-mode {
  margin-top: 12px;
  text-align: center;
}
</style>
