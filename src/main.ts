import App from './App.svelte';
import { mount } from 'svelte';
import './app.css';

const target = document.getElementById('app');

if (!target) {
  throw new Error('Failed to find root element');
}

const app = mount(App, { target });

export default app;
