import { writable } from "svelte/store";

export const toast = writable<{
  message: string;
  tone: "success" | "error";
} | null>(null);

export function showToast(
  message: string,
  tone: "success" | "error" = "success"
) {
  toast.set({ message, tone });
  setTimeout(() => {
    toast.set(null);
  }, 3200);
}
