import assert from "node:assert/strict";
import { test } from "node:test";
import { get } from "svelte/store";
import { createWorkspaceController } from "./workspaceController.ts";
import { createWorkspaceAnnotationObserver } from "./workspaceAnnotations.ts";
import { forgetAnnotationVersion, markAnnotationsChanged, markBulkAnnotationsChanged } from "./annotationVersions.ts";

test("restoring an existing revision baseline does not publish a refresh or replace saved scroll", () => {
  const projectId = "annotation-restored-baseline";
  markBulkAnnotationsChanged(projectId);
  markBulkAnnotationsChanged(projectId);
  const workspace = createWorkspaceController();
  const calls: string[] = [];
  // Components begin observing while currentProjectId is still null. Their
  // saved view then sets the project and scroll before the first page returns.
  const observer = createWorkspaceAnnotationObserver(workspace, {
    single: () => calls.push("single"), bulk: () => { calls.push("bulk"); workspace.scrollTop.set(0); },
  });
  workspace.currentProjectId.set(projectId);
  workspace.scrollTop.set(6624);
  observer.initialize();
  assert.deepEqual(calls, [], "the pre-existing bulk count is a baseline, not a new write");
  markAnnotationsChanged("another-case");
  assert.deepEqual(calls, []);
  markAnnotationsChanged(projectId);
  assert.deepEqual(calls, ["single"]);
  assert.equal(get(workspace.scrollTop), 6624);
  observer.suspend();
  markBulkAnnotationsChanged(projectId);
  observer.initialize();
  assert.deepEqual(calls, ["single"], "reinitialization must not fabricate a refresh either");
  workspace.dispose();
  markBulkAnnotationsChanged(projectId);
  assert.deepEqual(calls, ["single"]);
  forgetAnnotationVersion(projectId);
  forgetAnnotationVersion("another-case");
});

test("active observers follow only their own case and release subscriptions on disposal", () => {
  const a = createWorkspaceController(), b = createWorkspaceController();
  a.currentProjectId.set("observer-a"); b.currentProjectId.set("observer-b");
  const first: string[] = [], second: string[] = [];
  const left = createWorkspaceAnnotationObserver(a, { single: () => first.push("single"), bulk: () => first.push("bulk") });
  const right = createWorkspaceAnnotationObserver(b, { single: () => second.push("single"), bulk: () => second.push("bulk") });
  left.initialize(); right.initialize();
  markBulkAnnotationsChanged("observer-a");
  assert.deepEqual(first, ["single", "bulk"]);
  assert.deepEqual(second, []);
  a.dispose();
  markAnnotationsChanged("observer-a");
  markAnnotationsChanged("observer-b");
  assert.deepEqual(first, ["single", "bulk"]);
  assert.deepEqual(second, ["single"]);
  b.dispose();
  forgetAnnotationVersion("observer-a"); forgetAnnotationVersion("observer-b");
});
