import { onMounted, ref } from 'vue';
import 'https://cdnjs.cloudflare.com/ajax/libs/three.js/r134/three.min.js';
import 'https://cdn.jsdelivr.net/npm/vanta@latest/dist/vanta.dots.min.js';

export enum VantaType {
  DOTS = 'DOTS',
}

export interface VantaOptions {
  el: HTMLElement | string;
  [k: string]: unknown;
}

export interface VantaEffect {
  setOptions(options: Partial<VantaOptions>): void;
  resize(): void;
}

// by convention, composable function names start with "use"
export function useVantaBackground(type: VantaType, options: VantaOptions) {
  // state encapsulated and managed by the composable
  const vantaEffect = ref<VantaEffect | null>(null);

  function initVanta() {
    if (!window.VANTA) {
      return;
    }

    vantaEffect.value = window.VANTA[type](options);
  }

  onMounted(() => {
    initVanta();
  });

  // expose managed state as return value
  return { vantaEffect };
}
