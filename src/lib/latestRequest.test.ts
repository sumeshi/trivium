import assert from "node:assert/strict";
import { test } from "node:test";

import { createLatestRequest } from "./latestRequest.ts";

test("only the newest ticket is current", () => {
  const requests = createLatestRequest();
  const first = requests.next();
  const second = requests.next();
  assert.equal(requests.isCurrent(first), false);
  assert.equal(requests.isCurrent(second), true);
});

test("invalidate and dispose reject in-flight responses", () => {
  const requests = createLatestRequest();
  const ticket = requests.next();
  requests.invalidate();
  assert.equal(requests.isCurrent(ticket), false);

  const after = requests.next();
  assert.equal(requests.isCurrent(after), true);
  requests.dispose();
  assert.equal(requests.isCurrent(after), false);
});
