FROM node:20-alpine
WORKDIR /app

RUN npm install -g pnpm

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml tsconfig.json tsconfig.base.json ./
COPY packages ./packages
COPY apps ./apps
COPY workers ./workers

RUN pnpm install --frozen-lockfile

USER node

ENV NODE_ENV=production
EXPOSE 8081

CMD ["pnpm", "--filter", "@nexusgrid/auth-service", "start"]
