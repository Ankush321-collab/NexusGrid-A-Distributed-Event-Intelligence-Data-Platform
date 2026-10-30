FROM node:20-alpine AS builder
WORKDIR /app
RUN npm install -g pnpm
COPY . .
RUN pnpm install --frozen-lockfile
RUN pnpm --filter @nexusgrid/ingestion-service build

FROM node:20-alpine AS runner
WORKDIR /app
USER 10001
COPY --from=builder /app .
EXPOSE 8080
CMD ["node", "apps/ingestion-service/dist/index.js"]

