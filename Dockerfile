FROM node:22-alpine

WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev

COPY . .

EXPOSE 3200

HEALTHCHECK --interval=15s --timeout=10s --start-period=20s --retries=5 \
  CMD wget -qO- http://127.0.0.1:3200/health || exit 1

CMD ["node", "server.js"]
