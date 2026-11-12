<script lang="ts">
  import { get } from "svelte/store";
  import { onDestroy } from "svelte";
  import type { Backend } from "../../backend";
  import type { ColumnKind } from "../../types";
  import type { EditorCondition } from "../../stores/condition";
  import { conditionValues, isIncompleteTextCondition } from "../../stores/condition";
  import { conditionValueHelp, createConditionValueChecker, isOrderedCondition, utcPickerValue } from "../../conditionValueInput";
  import type { ConditionValueCheckState } from "../../conditionValueInput";
  import { useWorkspace } from "../../workspaceContext";
  const { backend, projectDetail, patchCondition } = useWorkspace();

  export let node: EditorCondition;
  let state: ConditionValueCheckState | null = null;
  let checkedKey = "";
  let checkedBackend: Backend | null = null;
  let pickedDate = "";
  let pickedTime = "";
  let pickerOpen = false;
  let inputEl: HTMLInputElement;
  const checker = createConditionValueChecker({
    validate: (input) => {
      const service = get(backend);
      if (!service?.validateConditionValues) return Promise.reject(new Error("Format checking is unavailable."));
      return service.validateConditionValues(input);
    },
    publish: (next) => { state = next; },
  });
  onDestroy(() => checker.dispose());

  $: kind = ($projectDetail?.column_kinds?.[node.column] ?? "string") as ColumnKind;
  $: help = conditionValueHelp(kind, node.op, node.exact);
  $: ordered = isOrderedCondition(node.op);
  $: key = JSON.stringify([$projectDetail?.project.meta.id, node.id, node.column, kind, node.op, node.value, node.exact]);
  $: if ($backend && (key !== checkedKey || $backend !== checkedBackend)) {
    checkedKey = key;
    checkedBackend = $backend;
    checker.schedule({ kind, op: node.op, values: conditionValues(node) });
  }
  $: draft = isIncompleteTextCondition(node);
  $: invalid = state?.status === "checked" && !state.result.valid;
  $: checkedValues = state?.status === "checked" ? state.result.values : [];
  $: errors = checkedValues.flatMap((value, index) => value.error
    ? [`${checkedValues.length > 1 ? `Value ${index + 1}: ` : ""}${value.error}`] : []);
  $: validationHint = state?.status === "checked" ? state.result.hint : null;
  $: accepted = state?.status === "checked" && state.result.valid && (ordered || node.op === "regex");
  $: acceptedLabel = node.op === "regex" ? "Regular expression accepted."
    : kind === "datetime" ? "Timestamp format accepted."
      : kind === "int" ? "Integer format accepted." : "Comparison format accepted.";
  $: pickerValue = utcPickerValue(pickedDate, pickedTime);

  const setValue = (value: string) => patchCondition(node.id, { value, exact: false });
  const insertPicker = () => {
    if (!pickerValue) return;
    setValue(pickerValue);
    pickerOpen = false;
    inputEl?.focus();
  };
</script>

<div class="condition-value-input">
  <div class="value-controls">
    <input
      class="condition-value"
      bind:this={inputEl}
      type="text"
      inputmode={kind === "int" && ordered ? "numeric" : undefined}
      value={node.value}
      placeholder={help.placeholder}
      on:input={(event) => setValue(event.currentTarget.value)}
      aria-label="Value"
      aria-describedby={`condition-help-${node.id} condition-check-${node.id}`}
      aria-invalid={invalid ? "true" : undefined}
      data-condition-id={node.id}
      autocomplete="off"
      autocapitalize="off"
      spellcheck="false"
    />
    {#if kind === "datetime" && ordered}
      <button type="button" class="date-assist" aria-label="Choose UTC date and time" title="Choose a date and time in UTC"
        aria-controls={`condition-picker-${node.id}`} aria-expanded={pickerOpen} on:click={() => { pickerOpen = !pickerOpen; }}>
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" aria-hidden="true"><rect x="3" y="5" width="18" height="16" rx="2" /><path d="M16 3v4M8 3v4M3 11h18m-12 4h2m3 0h2" /></svg>
      </button>
    {/if}
  </div>
  <p id={`condition-help-${node.id}`} class="value-help">{help.hint}</p>
  <div id={`condition-check-${node.id}`} class="value-check" class:invalid role="status" aria-live="polite" aria-atomic="true">
    {#if draft}
      <span>Enter text to apply this rule.</span>
    {:else if state?.status === "checking"}
      <span>Checking format…</span>
    {:else if invalid}
      <span>{errors.join(" ") || validationHint || "This value is not valid for the comparison."} Check the value before relying on the result.</span>
    {:else if state?.status === "unavailable"}
      <span>Format check unavailable. The text is preserved.</span>
    {:else if accepted}
      <span>{acceptedLabel}</span>
    {/if}
    {#if validationHint && !draft && !invalid && validationHint !== help.hint}<span class="engine-hint">{validationHint}</span>{/if}
  </div>
  {#if pickerOpen && kind === "datetime" && ordered}
    <div id={`condition-picker-${node.id}`} class="date-picker" role="group" aria-label="UTC date and time helper">
      <label>UTC date<input type="date" bind:value={pickedDate} aria-label="UTC date" /></label>
      <label>UTC time<input type="time" step="1" bind:value={pickedTime} aria-label="UTC time" /></label>
      <button type="button" class="insert-date" disabled={!pickerValue} on:click={insertPicker}>Insert UTC</button>
      <p>The picker inserts a new UTC value (Z). Edit the text for fractional seconds or another timezone; opening this helper keeps the current value.</p>
    </div>
  {/if}
</div>

<style>
  .condition-value-input { min-width: 0; display: flex; flex-direction: column; gap: 3px; }
  .value-controls { display: flex; gap: 3px; align-items: center; min-width: 0; }
  input.condition-value { flex: 1; min-width: 0; width: 100%; height: 27px; padding: 0 6px; border: 1px solid var(--border); border-radius: 4px; background: var(--surface); color: var(--fg); font-size: 11px; }
  input.condition-value[aria-invalid="true"] { border-color: var(--danger); }
  .value-help, .value-check { margin: 0; font-size: 10px; line-height: 1.45; color: var(--muted); overflow-wrap: anywhere; }
  .value-check:empty { display: none; }
  .value-check.invalid { color: var(--danger); }
  .engine-hint { display: block; }
  .date-assist { display: inline-flex; align-items: center; justify-content: center; flex: 0 0 27px; height: 27px; padding: 0; border: 1px solid var(--border); border-radius: 4px; color: var(--muted); background: var(--surface); }
  .date-assist:hover { color: var(--fg); border-color: var(--border-strong); }
  input:focus-visible, button:focus-visible { outline: 1px solid var(--accent-border); outline-offset: 1px; }
  .date-picker { display: flex; flex-wrap: wrap; align-items: end; gap: 5px; padding: 6px; border: 1px solid var(--border); border-radius: 4px; background: var(--surface-weak); }
  .date-picker label { flex: 1; min-width: 110px; display: flex; flex-direction: column; gap: 2px; color: var(--muted); font-size: 10px; }
  .date-picker input { min-width: 0; width: 100%; height: 27px; padding: 0 4px; font-size: 11px; color: var(--fg); background: var(--surface); border: 1px solid var(--border); border-radius: 4px; }
  .date-picker p { flex-basis: 100%; margin: 0; color: var(--muted); font-size: 10px; line-height: 1.45; }
  .insert-date { height: 27px; padding: 0 6px; border: 1px solid var(--accent-border); border-radius: 4px; background: var(--accent-soft); color: var(--fg); font-size: 10px; }
  .insert-date:disabled { opacity: .5; }
</style>
