import { createPinia } from 'pinia';
import { createApp } from "vue";
import App from "./App.vue";
import { createNaiveUI } from "./naive";

import router from "@/router";
// Analytics mode - no authentication needed

const app = createApp(App);
app.use(createNaiveUI());
app.use(router);    
app.use(createPinia());                    
app.mount("#app");