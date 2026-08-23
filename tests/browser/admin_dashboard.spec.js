const { test, expect } = require('@playwright/test');

test('mobile dashboard has labelled numeric scales and static legends', async ({ page }) => {
  await page.goto('/admin/login');
  await page.locator('input[name="username"]').fill('admin');
  await page.locator('input[name="password"]').fill('playwright-password');
  await page.locator('button[type="submit"]').click();
  await page.waitForURL('**/admin');

  for (const id of ['growth', 'delivery', 'engagement', 'llm']) {
    const chart = page.locator(`#${id}`);
    await expect(chart).toHaveAttribute('viewBox', '0 0 600 220');
    await expect(chart.locator('.axis-y')).toHaveCount(3);
    await expect(chart.locator('.axis-x')).toHaveCount(3);
    expect(await chart.evaluate(node => node.getBoundingClientRect().width <= window.innerWidth)).toBeTruthy();
    await expect(chart.locator('.axis-x').first()).toHaveText(/^\d{2}\.\d{2}$/);
  }

  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBeTruthy();

  await expect(page.locator('#growth-period')).toContainText(/шкала слева/i);
  await expect(page.locator('#rag-configuration')).toContainText(/Базовый поиск|Канарейка/);
  const growthLegend = page.getByRole('list', { name: 'Легенда графика роста' });
  await expect(growthLegend).toHaveText(/Пользователи.*Активные.*Подписки/);
  await expect(growthLegend.getByRole('button')).toHaveCount(0);
  await expect(page.getByRole('tab')).toHaveCount(0);
});
