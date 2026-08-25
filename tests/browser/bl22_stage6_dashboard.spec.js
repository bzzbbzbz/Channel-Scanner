const { test, expect } = require('@playwright/test');

test('stage 6 browser views metadata and idempotently replays one dead letter', async ({ page }) => {
  const deadLetterId = process.env.BL22_DEAD_LETTER_ID;
  test.skip(!deadLetterId, 'requires the BL-22 stage-6 harness');

  await page.goto('/admin/login');
  await page.locator('input[name="username"]').fill('bl22-admin');
  await page.locator('input[name="password"]').fill('bl22-stage6-password');
  await page.locator('button[type="submit"]').click();
  await page.waitForURL('**/admin');
  await expect(page.locator('#reliability')).toContainText('Reliability / DLQ');
  const row = page.locator(`[data-dead-letter-id="${deadLetterId}"]`);
  await expect(row).toContainText('digest_message');
  await row.getByRole('button', {name: 'Детали'}).click();
  const detail = page.locator('#dead-letter-detail');
  await expect(detail).toBeVisible();
  await expect(detail).toContainText(deadLetterId);
  await expect(detail).not.toContainText('BL22_STAGE6_PRIVATE_CONTENT');

  const responsePromise = page.waitForResponse(response =>
    response.url().endsWith(`/admin/api/dead-letters/${deadLetterId}/replay`) && response.request().method() === 'POST'
  );
  await detail.getByRole('button', {name: 'Повторить эту работу'}).click();
  const firstResponse = await responsePromise;
  expect(firstResponse.ok()).toBeTruthy();
  const first = await firstResponse.json();
  expect(first.result).toBe('replayed');
  await expect(detail).toContainText('bl22-admin / replayed / gen 2');

  const requestHeaders = firstResponse.request().headers();
  const secondResponse = await page.request.post(firstResponse.url(), {
    headers: {
      'X-CSRF-Token': requestHeaders['x-csrf-token'],
      'Idempotency-Key': requestHeaders['idempotency-key'],
    },
  });
  expect(secondResponse.ok()).toBeTruthy();
  const second = await secondResponse.json();
  expect(second.replay_id).toBe(first.replay_id);
  expect(second.outbox_event_id).toBe(first.outbox_event_id);

  await row.getByRole('button', {name: 'Детали'}).click();
  await expect(detail).toContainText('bl22-admin / replayed / gen 2');
  await expect(detail.getByText(/bl22-admin \/ replayed \/ gen 2/)).toHaveCount(1);
});
