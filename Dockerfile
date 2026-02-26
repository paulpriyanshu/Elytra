# Stage 1: Rust/WASM Builder
FROM rust:latest AS rust-builder
WORKDIR /app
RUN cargo install wasm-pack
COPY packages/rust-core ./packages/rust-core
WORKDIR /app/packages/rust-core
RUN wasm-pack build --target web --no-opt

# Stage 2: Node.js Dependencies
FROM node:20-alpine AS deps
RUN apk add --no-cache libc6-compat
WORKDIR /app
COPY package.json package-lock.json ./
COPY apps/web/package.json ./apps/web/
COPY packages/rust-core/package.json ./packages/rust-core/
COPY packages/ui/package.json ./packages/ui/
COPY packages/eslint-config/package.json ./packages/eslint-config/
COPY packages/typescript-config/package.json ./packages/typescript-config/
RUN npm ci

# Stage 3: Builder

FROM node:20-alpine AS builder
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY . .
# Copy the pre-built WASM package from the rust-builder stage
COPY --from=rust-builder /app/packages/rust-core/pkg ./packages/rust-core/pkg
# Remove rust-core's build script so Turbo doesn't try to run wasm-pack again
RUN node -e "const p = require('./packages/rust-core/package.json'); delete p.scripts.build; require('fs').writeFileSync('./packages/rust-core/package.json', JSON.stringify(p, null, 2))"
# Now build — Turbo will skip rust-core since it has no build script
RUN npx turbo run build --filter=web...

# Stage 4: Runner
FROM node:20-alpine AS runner
WORKDIR /app
ENV NODE_ENV=production
RUN addgroup --system --gid 1001 nodejs
RUN adduser --system --uid 1001 nextjs

# Copy standalone build and static files
COPY --from=builder /app/apps/web/public ./apps/web/public
COPY --from=builder --chown=nextjs:nodejs /app/apps/web/.next/standalone ./
COPY --from=builder --chown=nextjs:nodejs /app/apps/web/.next/static ./apps/web/.next/static

USER nextjs
EXPOSE 3000
ENV PORT=3000

CMD ["node", "apps/web/server.js"]