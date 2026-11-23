import { createRouter, createWebHistory } from 'vue-router'

import DeliveryIndex from '@/views/DeliveryIndex.vue'
import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/delivery-index', name: 'delivery-index', component: DeliveryIndex },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
