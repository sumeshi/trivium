<script lang="ts">
  import { toast } from '../../utils/toast';
</script>

{#if $toast}
  <div class={`toast ${$toast.tone}`} role="status">
    {$toast.message}
  </div>
{/if}
