const { test, expect } = require('@playwright/test');

test('mobile dashboard has labelled numeric scales and static legends', async ({ page }) => {
  const kafkaRequests = [];
  page.on('request', request => {
    if (request.url().endsWith('/admin/api/kafka/operations')) kafkaRequests.push(request);
  });
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
  await expect(page.locator('#reliability')).toContainText('Reliability / DLQ');
  await expect(page.locator('#dead-letters')).toContainText('DLQ пуста');
  const growthLegend = page.getByRole('list', { name: 'Легенда графика роста' });
  await expect(growthLegend).toHaveText(/Пользователи.*Активные.*Подписки/);
  await expect(growthLegend.getByRole('button')).toHaveCount(0);
  const overviewTab = page.getByRole('tab', { name: 'Обзор' });
  const kafkaTab = page.getByRole('tab', { name: 'Kafka' });
  await expect(page.getByRole('tab')).toHaveCount(2);
  await expect(overviewTab).toHaveAttribute('aria-selected', 'true');
  await expect(page.locator('#panel-overview')).toBeVisible();
  await expect(page.locator('#panel-kafka')).toBeHidden();
  expect(kafkaRequests).toHaveLength(0);

  const firstKafkaRequest = page.waitForRequest('**/admin/api/kafka/operations');
  await kafkaTab.click();
  await firstKafkaRequest;
  await expect(kafkaTab).toHaveAttribute('aria-selected', 'true');
  await expect(page.locator('#panel-overview')).toBeHidden();
  await expect(page.locator('#panel-kafka')).toBeVisible();
  await expect(page.locator('#kafka-status-cards')).toContainText('Shadow mode');
  await expect(page.locator('#kafka-status-cards')).toContainText('legacy');
  await expect(page.locator('#kafka-status-cards')).toContainText('12 мс');
  await expect(page.locator('#kafka-roles')).toContainText('outbox-relay');
  await expect(page.locator('#kafka-topics')).toContainText('digest.schedule.v1');
  await expect(page.locator('#kafka-mode-note')).toContainText('Отсутствующие consumer groups ожидаемы');
  await expect(page.locator('#kafka-groups')).toContainText('digest-renderer-v1');
  await expect(page.locator('#kafka-queues')).toContainText('Неопубликованный outbox');
  await expect(page.locator('#kafka-queues')).toContainText('2');
  await expect(page.locator('#kafka-queues')).toContainText('самая старая: 20 мин');
  await expect(page.locator('#kafka-queues')).toContainText('outbox: 1');
  await expect(page.locator('#kafka-errors')).toContainText('PublishTimeout');
  await expect(page.locator('.status-healthy').first()).toBeVisible();
  await expect(page.locator('.status-warning').first()).toBeVisible();
  await expect(page.locator('.status-error').first()).toBeVisible();
  await expect(page.locator('#panel-kafka').getByRole('button', { name: /логи|payload|токен/i })).toHaveCount(0);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBeTruthy();

  await page.evaluate(() => renderKafka({mode:{kafka_enabled:true,reliable_enabled:true},broker:{status:'healthy'}}));
  await expect(page.locator('#kafka-mode-note')).toContainText('Reliable mode');
  await expect(page.locator('#kafka-mode-note')).not.toContainText('Shadow mode');
  await page.evaluate(() => renderKafka({mode:{kafka_enabled:false,reliable_enabled:false},broker:{status:'disabled'}}));
  await expect(page.locator('#kafka-mode-note')).toContainText('Kafka выключена');
  await expect(page.locator('#kafka-mode-note')).not.toContainText('Shadow mode');
  await page.evaluate(() => renderKafka({mode:{kafka_enabled:true,reliable_enabled:false},broker:{status:'unavailable'}}));
  await expect(page.locator('#kafka-mode-note')).toContainText('Состояние Kafka недоступно');
  await expect(page.locator('#panel-kafka')).not.toContainText('Shadow mode');
  await page.evaluate(() => renderKafka({mode:{kafka_enabled:true,reliable_enabled:false,memory_enabled:false},broker:{status:'available'}}));
  await expect(page.locator('#kafka-mode-note')).toContainText('Режим не подтверждён');
  await expect(page.locator('#panel-kafka')).not.toContainText('Shadow mode');

  const manualKafkaRequest = page.waitForRequest('**/admin/api/kafka/operations');
  await page.getByRole('button', { name: 'Обновить' }).click();
  await manualKafkaRequest;
  expect(kafkaRequests.length).toBeGreaterThanOrEqual(2);

  await kafkaTab.focus();
  await kafkaTab.press('ArrowLeft');
  await expect(overviewTab).toBeFocused();
  await expect(overviewTab).toHaveAttribute('aria-selected', 'true');
  await expect(page.locator('#panel-overview')).toBeVisible();
  await expect(page.locator('#panel-kafka')).toBeHidden();
});
