import { defineConfig } from 'vitest/config';
import path from 'path';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    testTimeout: 20000,
  },
  resolve: {
    alias: {
      '@nexusgrid/contracts': path.resolve(__dirname, './packages/contracts/src'),
      '@nexusgrid/config': path.resolve(__dirname, './packages/config/src'),
      '@nexusgrid/observability': path.resolve(__dirname, './packages/observability/src'),
      '@nexusgrid/database': path.resolve(__dirname, './packages/database/src'),
    },
  },
});

