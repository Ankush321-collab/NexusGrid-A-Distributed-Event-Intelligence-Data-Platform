FROM node:20-alpine AS builder
WORKDIR /app
RUN npm install -g pnpm
COPY . .
RUN pnpm install --frozen-lockfile
RUN pnpm --filter @nexusgrid/auth-service build

FROM node:20-alpine AS runner
WORKDIR /app
USER 10001
COPY --from=builder /app .
EXPOSE 8081
CMD ["node", "apps/auth-service/dist/index.js"]

