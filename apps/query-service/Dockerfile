FROM node:20-alpine AS builder
WORKDIR /app
RUN npm install -g pnpm
COPY . .
RUN pnpm install --frozen-lockfile
RUN pnpm --filter @nexusgrid/query-service build

FROM node:20-alpine AS runner
WORKDIR /app
USER 10001
COPY --from=builder /app .
EXPOSE 8083
CMD ["node", "apps/query-service/dist/index.js"]
