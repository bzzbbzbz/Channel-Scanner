const { defineConfig } = require('@playwright/test');

module.exports = defineConfig({
  testDir: './tests/browser',
  testMatch: 'bl22_stage6_dashboard.spec.js',
  timeout: 30_000,
  workers: 1,
  use: {
    baseURL: process.env.BL22_ADMIN_URL,
    browserName: 'chromium',
    headless: true,
    trace: 'retain-on-failure',
  },
});
