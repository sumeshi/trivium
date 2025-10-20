<script lang="ts">
  import { toast } from '../../utils/toast';
</script>

{#if $toast}
  <div
    class={`fixed bottom-6 right-6 z-50 rounded-xl border px-4 py-3 text-sm shadow-lg backdrop-blur ${
      $toast.tone === 'error'
        ? 'border-rose-500/60 bg-rose-500/10 text-rose-200'
        : 'border-emerald-500/60 bg-emerald-500/10 text-emerald-100'
    }`}
  >
    {$toast.message}
  </div>
{/if}
